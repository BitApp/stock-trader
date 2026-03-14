use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::mpsc::{self, RecvTimeoutError},
    time::{Duration, Instant},
};

use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, Utc, Weekday};
use chrono_tz::Tz;
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use tracing::{error, info, warn};
use trading_core::{
    AppConfig, DefaultsConfig, Result, ScheduleOverduePolicy, TaskConfig, TaskScheduleConfig,
    TradeBotError, TradingEngine,
};

pub fn watch(
    config_path: Option<PathBuf>,
    config_dir: Option<PathBuf>,
    poll_seconds: u64,
) -> Result<()> {
    let source = WatchSource::from_args(config_path, config_dir)?;
    let poll_interval = Duration::from_secs(poll_seconds.max(1));
    let reload_debounce = Duration::from_secs(1);
    let mut state = WatchState::load(source.clone())?;
    let (_watcher, rx) = build_watcher(&source)?;
    let mut pending_reload_at: Option<Instant> = None;

    info!(
        source = %source.display(),
        timezone = %state.schedule_timezone(),
        poll_seconds = poll_interval.as_secs(),
        reload_debounce_seconds = reload_debounce.as_secs(),
        watch_root = %source.watch_root().display(),
        "watching config for scheduled tasks"
    );
    state.log_scheduled_tasks("loaded");

    loop {
        let now = Instant::now();
        let wait = next_watch_wait(poll_interval, pending_reload_at, now);
        match rx.recv_timeout(wait) {
            Ok(Ok(event)) => {
                if source.should_reload_for_event(&event) {
                    drain_ready_events(&rx);
                    pending_reload_at = Some(Instant::now() + reload_debounce);
                }
            }
            Ok(Err(err)) => {
                warn!(source = %source.display(), error = %err, "config watch event error");
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                return Err(TradeBotError::Config(
                    "file watcher disconnected unexpectedly".into(),
                ));
            }
        }
        if pending_reload_at.is_some_and(|deadline| Instant::now() >= deadline) {
            pending_reload_at = None;
            state.reload_if_changed();
        }
        state.run_due_tasks();
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
            (Some(path), None) => Ok(Self::File(resolve_watch_path(path)?)),
            (None, Some(path)) => Ok(Self::Dir(resolve_watch_path(path)?)),
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

    fn watch_root(&self) -> PathBuf {
        match self {
            Self::File(path) => path
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| path.clone()),
            Self::Dir(path) => path.clone(),
        }
    }

    fn should_reload_for_event(&self, event: &Event) -> bool {
        match self {
            Self::File(path) => {
                let watch_root = path.parent().unwrap_or(path.as_path());
                event.paths.is_empty()
                    || event.paths.iter().any(|changed| {
                        changed == path
                            || changed == watch_root
                            || changed.parent() == Some(watch_root)
                    })
            }
            Self::Dir(dir) => {
                event.paths.is_empty()
                    || event
                        .paths
                        .iter()
                        .any(|changed| changed.parent() == Some(dir) || changed == dir)
            }
        }
    }
}

fn resolve_watch_path(path: PathBuf) -> Result<PathBuf> {
    match fs::canonicalize(&path) {
        Ok(path) => Ok(path),
        Err(_) if path.is_absolute() => Ok(path),
        Err(_) => Ok(std::env::current_dir()?.join(path)),
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
    task_observed_at: BTreeMap<String, DateTime<Utc>>,
    last_checked_at: Option<DateTime<Utc>>,
}

impl WatchState {
    fn load(source: WatchSource) -> Result<Self> {
        let snapshot = source.load_snapshot()?;
        let observed_at = Utc::now();

        Ok(Self {
            engine: crate::build_engine(snapshot.config.clone()),
            task_observed_at: snapshot
                .config
                .tasks
                .iter()
                .map(|task| (task.name.clone(), observed_at))
                .collect(),
            config: snapshot.config,
            source,
            last_fingerprint: snapshot.fingerprint,
            last_attempted: BTreeMap::new(),
            last_checked_at: None,
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

        let observed_at = Utc::now();
        self.last_fingerprint = snapshot.fingerprint;
        self.last_attempted.retain(|task_name, _| {
            snapshot
                .config
                .tasks
                .iter()
                .any(|task| task.name == *task_name)
        });
        self.task_observed_at.retain(|task_name, _| {
            snapshot
                .config
                .tasks
                .iter()
                .any(|task| task.name == *task_name)
        });
        for task in &snapshot.config.tasks {
            self.task_observed_at
                .entry(task.name.clone())
                .or_insert(observed_at);
        }
        self.engine = crate::build_engine(snapshot.config.clone());
        self.config = snapshot.config;
        info!(
            source = %self.source.display(),
            timezone = %self.schedule_timezone(),
            "reloaded config and applied updated schedules"
        );
        self.log_scheduled_tasks("updated");
    }

    fn run_due_tasks(&mut self) {
        let now_utc = Utc::now();
        let now = now_utc.with_timezone(&self.schedule_timezone());
        let today = now.date_naive();
        for task in &self.config.tasks {
            let last_attempted = self.last_attempted.get(&task.name).copied();
            let previous_check = self
                .last_checked_at
                .map(|timestamp| timestamp.with_timezone(&self.schedule_timezone()))
                .map(|timestamp| (timestamp.date_naive(), timestamp.time()));
            let observed_at = self
                .task_observed_at
                .get(&task.name)
                .copied()
                .map(|timestamp| timestamp.with_timezone(&self.schedule_timezone()))
                .map(|timestamp| (timestamp.date_naive(), timestamp.time()));
            let due_window_start = latest_boundary(previous_check, observed_at);
            if !task_is_due(
                task,
                today,
                now.weekday(),
                now.time(),
                due_window_start,
                last_attempted,
            ) {
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
        self.last_checked_at = Some(now_utc);
    }

    fn schedule_timezone(&self) -> Tz {
        self.config
            .defaults
            .parse_timezone()
            .expect("validated config must contain a valid timezone")
    }

    fn log_scheduled_tasks(&self, verb: &str) {
        let scheduled_tasks = scheduled_task_descriptions(&self.config);
        let (enabled_count, total_count) = scheduled_task_counts(&self.config);

        info!(
            source = %self.source.display(),
            timezone = %self.schedule_timezone(),
            enabled = enabled_count,
            total = total_count,
            "scheduled tasks {verb}: {enabled_count}/{total_count} enabled"
        );
        for (index, task) in scheduled_tasks.into_iter().enumerate() {
            info!(
                source = %self.source.display(),
                "active task {}/{}: {}",
                index + 1,
                enabled_count,
                task
            );
        }
    }
}

fn scheduled_task_descriptions(config: &AppConfig) -> Vec<String> {
    config
        .tasks
        .iter()
        .filter_map(|task| {
            let schedule = task.schedule.as_ref()?;
            if !schedule.enabled {
                return None;
            }
            Some(format_active_scheduled_task(task, schedule))
        })
        .collect()
}

fn format_active_scheduled_task(task: &TaskConfig, schedule: &TaskScheduleConfig) -> String {
    format!(
        "{} | broker={} | action={} | {} | overdue={}",
        task.name,
        task.broker,
        task_action_as_str(task.action),
        schedule_when_description(schedule),
        overdue_policy_as_str(schedule.overdue_policy)
    )
}

fn scheduled_task_counts(config: &AppConfig) -> (usize, usize) {
    let total = config.tasks.len();
    let enabled = config
        .tasks
        .iter()
        .filter(|task| {
            task.schedule
                .as_ref()
                .is_some_and(|schedule| schedule.enabled)
        })
        .count();
    (enabled, total)
}

fn schedule_when_description(schedule: &TaskScheduleConfig) -> String {
    let weekday_summary = if schedule.weekdays.is_empty() {
        "all-days".to_string()
    } else {
        schedule
            .weekdays
            .iter()
            .map(|weekday| weekday.as_str())
            .collect::<Vec<_>>()
            .join(",")
    };
    let date_summary = schedule.date.as_deref().unwrap_or("any-date");

    format!(
        "when={} {} ({})",
        date_summary, schedule.time, weekday_summary
    )
}

fn schedule_description(schedule: &TaskScheduleConfig) -> String {
    let weekday_summary = if schedule.weekdays.is_empty() {
        "all-days".to_string()
    } else {
        schedule
            .weekdays
            .iter()
            .map(|weekday| weekday.as_str())
            .collect::<Vec<_>>()
            .join(",")
    };
    let date_summary = schedule
        .date
        .as_deref()
        .map(|date| format!("date={date}"))
        .unwrap_or_else(|| "date=any".to_string());

    format!(
        "{date_summary} time={} weekdays={} enabled={} overdue_policy={}",
        schedule.time,
        weekday_summary,
        schedule.enabled,
        overdue_policy_as_str(schedule.overdue_policy)
    )
}

fn overdue_policy_as_str(policy: ScheduleOverduePolicy) -> &'static str {
    match policy {
        ScheduleOverduePolicy::Run => "run",
        ScheduleOverduePolicy::Skip => "skip",
    }
}

fn task_action_as_str(action: trading_core::TaskAction) -> &'static str {
    match action {
        trading_core::TaskAction::Place => "place",
        trading_core::TaskAction::Cancel => "cancel",
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

fn build_watcher(
    source: &WatchSource,
) -> Result<(RecommendedWatcher, mpsc::Receiver<notify::Result<Event>>)> {
    let (tx, rx) = mpsc::channel();
    let mut watcher = notify::recommended_watcher(move |result| {
        let _ = tx.send(result);
    })
    .map_err(|err| TradeBotError::Config(format!("failed to start file watcher: {err}")))?;
    watcher
        .watch(&source.watch_root(), RecursiveMode::NonRecursive)
        .map_err(|err| TradeBotError::Config(format!("failed to watch config path: {err}")))?;
    Ok((watcher, rx))
}

fn drain_ready_events(rx: &mpsc::Receiver<notify::Result<Event>>) {
    while rx.try_recv().is_ok() {}
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
        if let Some(existing) = merged.brokers.get(&name) {
            if !broker_configs_equal(existing, &broker) {
                return Err(TradeBotError::Config(format!(
                    "config dir has conflicting broker `{name}` in `{}`",
                    path.display()
                )));
            }
            continue;
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

fn broker_configs_equal(
    left: &trading_core::BrokerConfig,
    right: &trading_core::BrokerConfig,
) -> bool {
    left.kind == right.kind
        && left.env_prefix == right.env_prefix
        && left.settings == right.settings
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
    due_window_start: Option<(NaiveDate, NaiveTime)>,
    last_attempted: Option<NaiveDate>,
) -> bool {
    let Some(schedule) = &task.schedule else {
        return false;
    };

    schedule_is_due(
        schedule,
        today,
        weekday,
        now_time,
        due_window_start,
        last_attempted,
    )
}

fn latest_boundary(
    left: Option<(NaiveDate, NaiveTime)>,
    right: Option<(NaiveDate, NaiveTime)>,
) -> Option<(NaiveDate, NaiveTime)> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn next_watch_wait(
    poll_interval: Duration,
    pending_reload_at: Option<Instant>,
    now: Instant,
) -> Duration {
    pending_reload_at
        .map(|deadline| deadline.saturating_duration_since(now).min(poll_interval))
        .unwrap_or(poll_interval)
}

fn schedule_is_due(
    schedule: &TaskScheduleConfig,
    today: NaiveDate,
    weekday: Weekday,
    now_time: NaiveTime,
    due_window_start: Option<(NaiveDate, NaiveTime)>,
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
        Ok(scheduled_time) => {
            if now_time < scheduled_time {
                return false;
            }

            match schedule.overdue_policy {
                ScheduleOverduePolicy::Run => true,
                ScheduleOverduePolicy::Skip => {
                    due_window_start.is_some_and(|(window_date, window_time)| {
                        window_date == today && window_time <= scheduled_time
                    })
                }
            }
        }
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    use chrono::{NaiveDate, NaiveTime, Weekday};
    use notify::{Event, EventKind};
    use trading_core::{AppConfig, ScheduleOverduePolicy, ScheduleWeekday, TaskScheduleConfig};

    use super::{
        WatchSource, collect_toml_paths, load_config_dir, next_watch_wait, resolve_watch_path,
        schedule_description, schedule_is_due, scheduled_task_counts, scheduled_task_descriptions,
    };

    fn weekday_only_schedule() -> TaskScheduleConfig {
        TaskScheduleConfig {
            date: None,
            time: "09:30".into(),
            weekdays: vec![ScheduleWeekday::Mon, ScheduleWeekday::Tue],
            enabled: true,
            overdue_policy: ScheduleOverduePolicy::Run,
        }
    }

    fn sample_config(task_name: &str, broker_name: &str, timezone: &str) -> String {
        format!(
            r#"[defaults]
timezone = "{timezone}"
default_tif = "day"

[brokers.{broker_name}]
kind = "ibkr"

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
            None,
        ));
        assert!(!schedule_is_due(
            &schedule,
            today,
            Weekday::Mon,
            NaiveTime::from_hms_opt(9, 31, 0).unwrap(),
            None,
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
            None,
        ));
        assert!(!schedule_is_due(
            &schedule,
            today,
            Weekday::Mon,
            NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            None,
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
            overdue_policy: ScheduleOverduePolicy::Run,
        };

        assert!(schedule_is_due(
            &schedule,
            NaiveDate::from_ymd_opt(2026, 3, 13).unwrap(),
            Weekday::Fri,
            NaiveTime::from_hms_opt(21, 31, 0).unwrap(),
            None,
            None,
        ));
        assert!(!schedule_is_due(
            &schedule,
            NaiveDate::from_ymd_opt(2026, 3, 14).unwrap(),
            Weekday::Sat,
            NaiveTime::from_hms_opt(21, 31, 0).unwrap(),
            None,
            None,
        ));
    }

    #[test]
    fn skip_overdue_policy_does_not_backfill_missed_runs() {
        let today = NaiveDate::from_ymd_opt(2026, 3, 16).unwrap();
        let mut schedule = weekday_only_schedule();
        schedule.overdue_policy = ScheduleOverduePolicy::Skip;

        assert!(!schedule_is_due(
            &schedule,
            today,
            Weekday::Mon,
            NaiveTime::from_hms_opt(9, 31, 0).unwrap(),
            None,
            None,
        ));
    }

    #[test]
    fn skip_overdue_policy_runs_when_watch_crosses_target_time() {
        let today = NaiveDate::from_ymd_opt(2026, 3, 16).unwrap();
        let mut schedule = weekday_only_schedule();
        schedule.overdue_policy = ScheduleOverduePolicy::Skip;

        assert!(schedule_is_due(
            &schedule,
            today,
            Weekday::Mon,
            NaiveTime::from_hms_opt(9, 31, 0).unwrap(),
            Some((today, NaiveTime::from_hms_opt(9, 29, 0).unwrap())),
            None,
        ));
        assert!(!schedule_is_due(
            &schedule,
            today,
            Weekday::Mon,
            NaiveTime::from_hms_opt(9, 31, 0).unwrap(),
            Some((today, NaiveTime::from_hms_opt(9, 30, 1).unwrap())),
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
    fn resolves_relative_watch_path_to_absolute_path() {
        let relative = PathBuf::from("config/oneshot-tasks");
        let resolved = resolve_watch_path(relative).unwrap();
        assert!(resolved.is_absolute());
        assert!(resolved.ends_with("config/oneshot-tasks"));
    }

    #[test]
    fn dir_watch_source_matches_absolute_event_paths() {
        let dir = resolve_watch_path(PathBuf::from("config/oneshot-tasks")).unwrap();
        let source = WatchSource::Dir(dir.clone());
        let event = Event {
            kind: EventKind::Any,
            paths: vec![dir.join("qqq.toml")],
            attrs: Default::default(),
        };

        assert!(source.should_reload_for_event(&event));
    }

    #[test]
    fn next_watch_wait_uses_pending_reload_deadline_when_sooner() {
        let now = Instant::now();
        let wait = next_watch_wait(
            Duration::from_secs(5),
            Some(now + Duration::from_secs(1)),
            now,
        );

        assert_eq!(wait, Duration::from_secs(1));
    }

    #[test]
    fn next_watch_wait_caps_elapsed_deadline_to_zero() {
        let now = Instant::now();
        let wait = next_watch_wait(
            Duration::from_secs(5),
            Some(now - Duration::from_millis(1)),
            now,
        );

        assert_eq!(wait, Duration::ZERO);
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

    #[test]
    fn load_config_dir_allows_duplicate_identical_brokers() {
        let dir = temp_dir("duplicate-broker");
        let left = sample_config("task-a", "shared", "UTC");
        let right = sample_config("task-b", "shared", "UTC");
        fs::write(dir.join("a.toml"), left).unwrap();
        fs::write(dir.join("b.toml"), right).unwrap();

        let snapshot = load_config_dir(&dir).unwrap();
        assert_eq!(snapshot.config.tasks.len(), 2);
        assert_eq!(snapshot.config.brokers.len(), 1);
        assert!(snapshot.config.brokers.contains_key("shared"));
    }

    #[test]
    fn load_config_dir_rejects_conflicting_duplicate_brokers() {
        let dir = temp_dir("conflicting-broker");
        let left = sample_config("task-a", "shared", "UTC");
        let right = sample_config("task-b", "shared", "UTC")
            .replace("kind = \"ibkr\"", "kind = \"longbridge\"");
        fs::write(dir.join("a.toml"), left).unwrap();
        fs::write(dir.join("b.toml"), right).unwrap();

        let err = load_config_dir(&dir).unwrap_err();
        assert!(err.to_string().contains("conflicting broker `shared`"));
    }

    #[test]
    fn scheduled_task_descriptions_omit_unscheduled_and_disabled_tasks() {
        let raw = r#"[defaults]
timezone = "UTC"

[brokers.paper]
kind = "ibkr"

[[tasks]]
name = "scheduled-a"
broker = "paper"
action = "place"
schedule = { time = "09:30", weekdays = ["mon"], overdue_policy = "skip" }
side = "buy"
pricing = { kind = "counterparty" }
shared_budget = { amount = 1000.0 }

[[tasks.symbols]]
ticker = "AAPL"
market = "us"
weight = 1.0

[[tasks]]
name = "manual-b"
broker = "paper"
action = "place"
side = "buy"
pricing = { kind = "counterparty" }
shared_budget = { amount = 1000.0 }

[[tasks.symbols]]
ticker = "MSFT"
market = "us"
weight = 1.0

[[tasks]]
name = "disabled-c"
broker = "paper"
action = "place"
schedule = { time = "09:30", weekdays = ["mon"], enabled = false }
side = "buy"
pricing = { kind = "counterparty" }
shared_budget = { amount = 1000.0 }

[[tasks.symbols]]
ticker = "GOOG"
market = "us"
weight = 1.0
"#;
        let config = AppConfig::from_toml(raw).unwrap();

        let descriptions = scheduled_task_descriptions(&config);
        assert_eq!(descriptions.len(), 1);
        assert!(descriptions[0].contains("scheduled-a"));
        assert!(descriptions[0].contains("overdue=skip"));
        assert!(!descriptions[0].contains("manual-b"));
        assert!(!descriptions[0].contains("disabled-c"));
    }

    #[test]
    fn scheduled_task_counts_include_disabled_and_manual_tasks_in_total() {
        let raw = r#"[defaults]
timezone = "UTC"

[brokers.paper]
kind = "ibkr"

[[tasks]]
name = "scheduled-a"
broker = "paper"
schedule = { time = "09:30", weekdays = ["mon"] }
side = "buy"
pricing = { kind = "counterparty" }
shared_budget = { amount = 1000.0 }

[[tasks.symbols]]
ticker = "AAPL"
market = "us"
weight = 1.0

[[tasks]]
name = "manual-b"
broker = "paper"
side = "buy"
pricing = { kind = "counterparty" }
shared_budget = { amount = 1000.0 }

[[tasks.symbols]]
ticker = "MSFT"
market = "us"
weight = 1.0

[[tasks]]
name = "disabled-c"
broker = "paper"
schedule = { time = "09:30", weekdays = ["mon"], enabled = false }
side = "buy"
pricing = { kind = "counterparty" }
shared_budget = { amount = 1000.0 }

[[tasks.symbols]]
ticker = "GOOG"
market = "us"
weight = 1.0
"#;
        let config = AppConfig::from_toml(raw).unwrap();

        assert_eq!(scheduled_task_counts(&config), (1, 3));
    }

    #[test]
    fn schedule_description_includes_date_and_all_days_when_unspecified() {
        let schedule = TaskScheduleConfig {
            date: None,
            time: "21:30".into(),
            weekdays: Vec::new(),
            enabled: true,
            overdue_policy: ScheduleOverduePolicy::Run,
        };

        let description = schedule_description(&schedule);
        assert!(description.contains("date=any"));
        assert!(description.contains("time=21:30"));
        assert!(description.contains("weekdays=all-days"));
        assert!(description.contains("overdue_policy=run"));
    }
}
