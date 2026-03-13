use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use chrono::{Datelike, NaiveDate, NaiveTime, Utc, Weekday};
use chrono_tz::Tz;
use tracing::{error, info, warn};
use trading_core::{
    AppConfig, DefaultsConfig, Result, TaskConfig, TaskScheduleConfig, TradeBotError, TradingEngine,
};

pub fn watch(
    config_path: Option<PathBuf>,
    config_dir: Option<PathBuf>,
    poll_seconds: u64,
) -> Result<()> {
    let source = WatchSource::from_args(config_path, config_dir)?;
    let poll_interval = Duration::from_secs(poll_seconds.max(1));
    let mut state = WatchState::load(source.clone())?;

    info!(
        source = %source.display(),
        timezone = %state.schedule_timezone(),
        poll_seconds = poll_interval.as_secs(),
        "watching config for scheduled tasks"
    );

    loop {
        state.reload_if_changed();
        state.run_due_tasks();
        thread::sleep(poll_interval);
    }
}

#[derive(Debug, Clone)]
enum WatchSource {
    File(PathBuf),
    Dir(PathBuf),
}

impl WatchSource {
    fn from_args(config_path: Option<PathBuf>, config_dir: Option<PathBuf>) -> Result<Self> {
        match (config_path, config_dir) {
            (Some(path), None) => Ok(Self::File(path)),
            (None, Some(path)) => Ok(Self::Dir(path)),
            (None, None) => Err(TradeBotError::Config(
                "watch requires either --config or --config-dir".into(),
            )),
            (Some(_), Some(_)) => Err(TradeBotError::Config(
                "watch accepts only one of --config or --config-dir".into(),
            )),
        }
    }

    fn load_snapshot(&self) -> Result<ConfigSnapshot> {
        match self {
            Self::File(path) => {
                let raw = fs::read_to_string(path)?;
                let config = AppConfig::from_toml(&raw)?;
                Ok(ConfigSnapshot {
                    config,
                    fingerprint: raw,
                })
            }
            Self::Dir(path) => load_config_dir(path),
        }
    }

    fn display(&self) -> String {
        match self {
            Self::File(path) => path.display().to_string(),
            Self::Dir(path) => format!("{}/*.toml", path.display()),
        }
    }
}

#[derive(Debug)]
struct ConfigSnapshot {
    config: AppConfig,
    fingerprint: String,
}

struct WatchState {
    source: WatchSource,
    config: AppConfig,
    engine: TradingEngine,
    last_fingerprint: String,
    last_attempted: BTreeMap<String, NaiveDate>,
}

impl WatchState {
    fn load(source: WatchSource) -> Result<Self> {
        let snapshot = source.load_snapshot()?;

        Ok(Self {
            engine: crate::build_engine(snapshot.config.clone()),
            config: snapshot.config,
            source,
            last_fingerprint: snapshot.fingerprint,
            last_attempted: BTreeMap::new(),
        })
    }

    fn reload_if_changed(&mut self) {
        let snapshot = match self.source.load_snapshot() {
            Ok(snapshot) => snapshot,
            Err(err) => {
                warn!(
                    source = %self.source.display(),
                    error = %err,
                    "failed to reload config; keeping previous valid config"
                );
                return;
            }
        };

        if snapshot.fingerprint == self.last_fingerprint {
            return;
        }

        self.last_fingerprint = snapshot.fingerprint;
        self.last_attempted.retain(|task_name, _| {
            snapshot
                .config
                .tasks
                .iter()
                .any(|task| task.name == *task_name)
        });
        self.engine = crate::build_engine(snapshot.config.clone());
        self.config = snapshot.config;
        info!(
            source = %self.source.display(),
            timezone = %self.schedule_timezone(),
            "reloaded config and applied updated schedules"
        );
    }

    fn run_due_tasks(&mut self) {
        let now = Utc::now().with_timezone(&self.schedule_timezone());
        let today = now.date_naive();
        for task in &self.config.tasks {
            let last_attempted = self.last_attempted.get(&task.name).copied();
            if !task_is_due(task, today, now.weekday(), now.time(), last_attempted) {
                continue;
            }

            self.last_attempted.insert(task.name.clone(), today);
            info!(
                task = %task.name,
                scheduled_for = task.schedule.as_ref().map(|schedule| schedule.time.as_str()).unwrap_or(""),
                "running scheduled task"
            );

            match self.engine.run_task(&task.name) {
                Ok(result) => {
                    crate::notify::notify_task_success(&self.config, task, &result);
                    info!(
                        task = %task.name,
                        broker = %result.broker_name,
                        action = ?result.action,
                        "scheduled task completed"
                    );
                    match serde_json::to_string_pretty(&result) {
                        Ok(json) => println!("{json}"),
                        Err(err) => error!(
                            task = %task.name,
                            error = %err,
                            "failed to serialize scheduled task result"
                        ),
                    }
                }
                Err(err) => {
                    crate::notify::notify_task_failure(&self.config, task, &err.to_string());
                    error!(task = %task.name, error = %err, "scheduled task failed");
                }
            }
        }
    }

    fn schedule_timezone(&self) -> Tz {
        self.config
            .defaults
            .parse_timezone()
            .expect("validated config must contain a valid timezone")
    }
}

fn load_config_dir(dir: &Path) -> Result<ConfigSnapshot> {
    let paths = collect_toml_paths(dir)?;
    if paths.is_empty() {
        return Err(TradeBotError::Config(format!(
            "config dir `{}` does not contain any .toml files",
            dir.display()
        )));
    }

    let mut fingerprint_parts = Vec::new();
    let mut merged = AppConfig {
        defaults: DefaultsConfig::default(),
        brokers: BTreeMap::new(),
        tasks: Vec::new(),
    };
    let mut defaults_source: Option<PathBuf> = None;

    for path in paths {
        let raw = fs::read_to_string(&path)?;
        fingerprint_parts.push(format!("FILE:{}\n{}", path.display(), raw));
        let config = AppConfig::from_toml(&raw)?;
        merge_config(&mut merged, config, &path, &mut defaults_source)?;
    }

    merged.validate()?;

    Ok(ConfigSnapshot {
        config: merged,
        fingerprint: fingerprint_parts.join("\n--\n"),
    })
}

fn collect_toml_paths(dir: &Path) -> Result<Vec<PathBuf>> {
    let metadata = fs::metadata(dir)?;
    if !metadata.is_dir() {
        return Err(TradeBotError::Config(format!(
            "`{}` is not a directory",
            dir.display()
        )));
    }

    let mut paths = fs::read_dir(dir)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "toml"))
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

fn merge_config(
    merged: &mut AppConfig,
    config: AppConfig,
    path: &Path,
    defaults_source: &mut Option<PathBuf>,
) -> Result<()> {
    if let Some(existing_source) = defaults_source.as_ref() {
        if !defaults_equal(&merged.defaults, &config.defaults) {
            return Err(TradeBotError::Config(format!(
                "config dir has conflicting defaults between `{}` and `{}`",
                existing_source.display(),
                path.display()
            )));
        }
    } else {
        merged.defaults = config.defaults.clone();
        *defaults_source = Some(path.to_path_buf());
    }

    for (name, broker) in config.brokers {
        if merged.brokers.contains_key(&name) {
            return Err(TradeBotError::Config(format!(
                "config dir has duplicate broker `{name}` in `{}`",
                path.display()
            )));
        }
        merged.brokers.insert(name, broker);
    }

    for task in config.tasks {
        if merged
            .tasks
            .iter()
            .any(|existing| existing.name == task.name)
        {
            return Err(TradeBotError::Config(format!(
                "config dir has duplicate task `{}` in `{}`",
                task.name,
                path.display()
            )));
        }
        merged.tasks.push(task);
    }

    Ok(())
}

fn defaults_equal(left: &DefaultsConfig, right: &DefaultsConfig) -> bool {
    left.timezone == right.timezone
        && left.default_tif == right.default_tif
        && email_defaults_equal(left.email.as_ref(), right.email.as_ref())
}

fn email_defaults_equal(
    left: Option<&trading_core::EmailTransportConfig>,
    right: Option<&trading_core::EmailTransportConfig>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => left.subject_prefix == right.subject_prefix,
        _ => false,
    }
}

fn task_is_due(
    task: &TaskConfig,
    today: NaiveDate,
    weekday: Weekday,
    now_time: NaiveTime,
    last_attempted: Option<NaiveDate>,
) -> bool {
    let Some(schedule) = &task.schedule else {
        return false;
    };

    schedule_is_due(schedule, today, weekday, now_time, last_attempted)
}

fn schedule_is_due(
    schedule: &TaskScheduleConfig,
    today: NaiveDate,
    weekday: Weekday,
    now_time: NaiveTime,
    last_attempted: Option<NaiveDate>,
) -> bool {
    if last_attempted == Some(today) {
        return false;
    }

    if let Ok(Some(date)) = schedule.parse_date() {
        if date != today {
            return false;
        }
    }

    if !schedule.is_enabled_on(weekday) {
        return false;
    }

    match schedule.parse_time() {
        Ok(scheduled_time) => now_time >= scheduled_time,
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use chrono::{NaiveDate, NaiveTime, Weekday};
    use trading_core::{ScheduleWeekday, TaskScheduleConfig};

    use super::{collect_toml_paths, load_config_dir, schedule_is_due};

    fn weekday_only_schedule() -> TaskScheduleConfig {
        TaskScheduleConfig {
            date: None,
            time: "09:30".into(),
            weekdays: vec![ScheduleWeekday::Mon, ScheduleWeekday::Tue],
            enabled: true,
        }
    }

    fn sample_config(task_name: &str, broker_name: &str, timezone: &str) -> String {
        format!(
            r#"[defaults]
timezone = "{timezone}"
default_tif = "day"

[brokers.{broker_name}]
kind = "ibkr"
base_url = "https://127.0.0.1:5000/v1/api"
account_id = "DU123456"
allow_insecure_tls = true

[[tasks]]
name = "{task_name}"
broker = "{broker_name}"
action = "place"
schedule = {{ time = "09:30", weekdays = ["mon"] }}
side = "buy"
pricing = {{ kind = "counterparty" }}
shared_budget = {{ amount = 1000.0 }}

[[tasks.symbols]]
ticker = "AAPL"
market = "us"
conid = "265598"
weight = 1.0
"#
        )
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("tradebot-watch-{label}-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn schedule_runs_once_after_target_time() {
        let today = NaiveDate::from_ymd_opt(2026, 3, 16).unwrap();
        let schedule = weekday_only_schedule();

        assert!(schedule_is_due(
            &schedule,
            today,
            Weekday::Mon,
            NaiveTime::from_hms_opt(9, 31, 0).unwrap(),
            None,
        ));
        assert!(!schedule_is_due(
            &schedule,
            today,
            Weekday::Mon,
            NaiveTime::from_hms_opt(9, 31, 0).unwrap(),
            Some(today),
        ));
    }

    #[test]
    fn schedule_skips_before_target_time_and_wrong_day() {
        let today = NaiveDate::from_ymd_opt(2026, 3, 15).unwrap();
        let schedule = weekday_only_schedule();

        assert!(!schedule_is_due(
            &schedule,
            today,
            Weekday::Sun,
            NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
            None,
        ));
        assert!(!schedule_is_due(
            &schedule,
            today,
            Weekday::Mon,
            NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            None,
        ));
    }

    #[test]
    fn schedule_runs_only_on_matching_date() {
        let schedule = TaskScheduleConfig {
            date: Some("2026-03-13".into()),
            time: "21:30".into(),
            weekdays: Vec::new(),
            enabled: true,
        };

        assert!(schedule_is_due(
            &schedule,
            NaiveDate::from_ymd_opt(2026, 3, 13).unwrap(),
            Weekday::Fri,
            NaiveTime::from_hms_opt(21, 31, 0).unwrap(),
            None,
        ));
        assert!(!schedule_is_due(
            &schedule,
            NaiveDate::from_ymd_opt(2026, 3, 14).unwrap(),
            Weekday::Sat,
            NaiveTime::from_hms_opt(21, 31, 0).unwrap(),
            None,
        ));
    }

    #[test]
    fn collect_toml_paths_sorts_and_filters() {
        let dir = temp_dir("collect");
        fs::write(dir.join("b.toml"), "x").unwrap();
        fs::write(dir.join("a.toml"), "x").unwrap();
        fs::write(dir.join("ignore.txt"), "x").unwrap();

        let paths = collect_toml_paths(&dir).unwrap();
        let names = paths
            .iter()
            .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["a.toml", "b.toml"]);
    }

    #[test]
    fn load_config_dir_merges_multiple_files() {
        let dir = temp_dir("merge");
        let left = sample_config("task-a", "broker_a", "UTC");
        let right = sample_config("task-b", "broker_b", "UTC");
        fs::write(dir.join("a.toml"), left).unwrap();
        fs::write(dir.join("b.toml"), right).unwrap();

        let snapshot = load_config_dir(&dir).unwrap();
        assert_eq!(snapshot.config.tasks.len(), 2);
        assert_eq!(snapshot.config.brokers.len(), 2);
    }

    #[test]
    fn load_config_dir_rejects_conflicting_defaults() {
        let dir = temp_dir("defaults");
        let left = sample_config("task-a", "broker_a", "Asia/Shanghai");
        let right = sample_config("task-b", "broker_b", "UTC");
        fs::write(dir.join("a.toml"), left).unwrap();
        fs::write(dir.join("b.toml"), right).unwrap();

        let err = load_config_dir(&dir).unwrap_err();
        assert!(err.to_string().contains("conflicting defaults"));
    }
}
