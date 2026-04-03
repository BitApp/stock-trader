use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::mpsc::{self, RecvTimeoutError},
    time::{Duration, Instant},
};

use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, Timelike, Utc, Weekday};
use chrono_tz::Tz;
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use tracing::{error, info, warn};
use trading_core::{
    AppConfig, ConfigFragment, DefaultsConfig, Result, ScheduleOverduePolicy, TaskConfig,
    TaskScheduleConfig, TradeBotError, TradingEngine, WatchConfig,
};

pub fn watch(
    config_path: Option<PathBuf>,
    config_dir: Option<PathBuf>,
    poll_seconds: u64,
) -> Result<()> {
    let source = WatchSource::from_args(config_path, config_dir)?;
    let mut state = WatchState::load(source.clone())?;
    let poll_interval = Duration::from_secs(poll_seconds.max(1));
    let (_watcher, rx) = build_watcher(&source)?;
    let mut pending_reload_at: Option<Instant> = None;

    info!(
        source = %source.display(),
        timezone = %state.schedule_timezone(),
        poll_seconds = poll_interval.as_secs(),
        reload_debounce_seconds = state.reload_debounce().as_secs(),
        watch_root = %source.watch_root().display(),
        "watching config for scheduled tasks"
    );
    state.log_scheduled_tasks("loaded");
    crate::notify::notify_task_list_loaded(&state.config, &source.display());

    loop {
        let now = Instant::now();
        let wait = next_watch_wait(poll_interval, pending_reload_at, now);
        match rx.recv_timeout(wait) {
            Ok(Ok(event)) => {
                if source.should_reload_for_event(&event) {
                    drain_ready_events(&rx);
                    pending_reload_at = Some(Instant::now() + state.reload_debounce());
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
        state.notify_task_list_confirm_if_due();
        state.run_due_tasks();
    }
}

pub fn load_app_config(
    config_path: Option<PathBuf>,
    config_dir: Option<PathBuf>,
) -> Result<AppConfig> {
    Ok(WatchSource::from_args(config_path, config_dir)?
        .load_snapshot()?
        .config)
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
    last_task_list_confirmed: Option<NaiveDate>,
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
            last_task_list_confirmed: None,
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

        let previous_config = self.config.clone();
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
        crate::notify::notify_task_list_changed(
            &previous_config,
            &self.config,
            &self.source.display(),
        );
    }

    fn notify_task_list_confirm_if_due(&mut self) {
        let now_utc = Utc::now();
        let now = now_utc.with_timezone(&self.schedule_timezone());
        let today = now.date_naive();
        let previous_check = self
            .last_checked_at
            .map(|timestamp| timestamp.with_timezone(&self.schedule_timezone()))
            .map(|timestamp| (timestamp.date_naive(), timestamp.time()));

        if !task_list_confirm_is_due(
            &self.config,
            today,
            now.weekday(),
            now.time(),
            previous_check,
            self.last_task_list_confirmed,
        ) {
            return;
        }

        self.last_task_list_confirmed = Some(today);
        if let Some(window) = task_list_confirm_window(&self.config, today, now.weekday()) {
            info!(
                source = %self.source.display(),
                lead_minutes = self.config.watch.task_list_confirm_lead_minutes,
                confirm_at = %window.confirm_time,
                first_task_at = %window.first_task_time,
                "sending task list confirmation notification"
            );
        }
        crate::notify::notify_task_list_confirm(&self.config, &self.source.display());
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

    fn reload_debounce(&self) -> Duration {
        Duration::from_secs(self.config.watch.reload_debounce_seconds)
    }

    fn log_scheduled_tasks(&self, verb: &str) {
        let scheduled_tasks = scheduled_task_descriptions(&self.config);
        let (enabled_count, total_count) = scheduled_task_counts(&self.config);
        let active_tasks = scheduled_tasks
            .iter()
            .filter(|task| task.enabled)
            .collect::<Vec<_>>();
        let disabled_tasks = scheduled_tasks
            .iter()
            .filter(|task| !task.enabled)
            .collect::<Vec<_>>();
        let active_count = active_tasks.len();
        let disabled_count = disabled_tasks.len();
        let manual_count = total_count.saturating_sub(scheduled_tasks.len());

        info!(
            source = %self.source.display(),
            timezone = %self.schedule_timezone(),
            enabled = enabled_count,
            total = total_count,
            disabled = disabled_count,
            manual = manual_count,
            "scheduled tasks {verb}: {enabled_count}/{total_count} enabled, {} disabled scheduled, {} manual",
            disabled_count,
            manual_count
        );
        if !active_tasks.is_empty() {
            info!(
                source = %self.source.display(),
                count = active_count,
                "active scheduled tasks"
            );
            for (index, task) in active_tasks.into_iter().enumerate() {
                info!(
                    source = %self.source.display(),
                    "ACTIVE {}/{}: {}",
                    index + 1,
                    active_count,
                    task.description
                );
            }
        }
        if !disabled_tasks.is_empty() {
            info!(
                source = %self.source.display(),
                count = disabled_count,
                "disabled scheduled tasks"
            );
            for (index, task) in disabled_tasks.into_iter().enumerate() {
                info!(
                    source = %self.source.display(),
                    "DISABLED {}/{}: {}",
                    index + 1,
                    disabled_count,
                    task.description
                );
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScheduledTaskDescription {
    description: String,
    enabled: bool,
}

fn scheduled_task_descriptions(config: &AppConfig) -> Vec<ScheduledTaskDescription> {
    config
        .tasks
        .iter()
        .filter_map(|task| {
            let schedule = task.schedule.as_ref()?;
            Some(ScheduledTaskDescription {
                description: format_scheduled_task(task, schedule),
                enabled: schedule.enabled,
            })
        })
        .collect()
}

fn format_scheduled_task(task: &TaskConfig, schedule: &TaskScheduleConfig) -> String {
    format!(
        "{} | broker={} | side={} | {} | overdue={}",
        task.name,
        task.broker,
        format_task_side(task.side),
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

#[cfg(test)]
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

fn format_task_side(side: Option<trading_core::OrderSide>) -> &'static str {
    match side {
        Some(trading_core::OrderSide::Buy) => "buy",
        Some(trading_core::OrderSide::Sell) => "sell",
        None => "n/a",
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
    let mut merged = ConfigFragment::default();
    let mut defaults_source: Option<PathBuf> = None;
    let mut watch_source: Option<PathBuf> = None;

    for path in paths {
        let raw = fs::read_to_string(&path)?;
        fingerprint_parts.push(format!("FILE:{}\n{}", path.display(), raw));
        let config = ConfigFragment::from_toml(&raw)?;
        merge_config(
            &mut merged,
            config,
            &path,
            &mut defaults_source,
            &mut watch_source,
        )?;
    }

    let config = AppConfig::from_fragment(merged)?;

    Ok(ConfigSnapshot {
        config,
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
    merged: &mut ConfigFragment,
    config: ConfigFragment,
    path: &Path,
    defaults_source: &mut Option<PathBuf>,
    watch_source: &mut Option<PathBuf>,
) -> Result<()> {
    if let Some(defaults) = config.defaults {
        if let Some(existing_source) = defaults_source.as_ref() {
            let existing_defaults = merged
                .defaults
                .as_ref()
                .expect("defaults_source implies merged defaults");
            if !defaults_equal(existing_defaults, &defaults) {
                return Err(TradeBotError::Config(format!(
                    "config dir has conflicting defaults between `{}` and `{}`",
                    existing_source.display(),
                    path.display()
                )));
            }
        } else {
            merged.defaults = Some(defaults);
            *defaults_source = Some(path.to_path_buf());
        }
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

    if let Some(watch) = config.watch {
        if let Some(existing_source) = watch_source.as_ref() {
            let existing_watch = merged
                .watch
                .as_ref()
                .expect("watch_source implies merged watch config");
            if !watch_configs_equal(existing_watch, &watch) {
                return Err(TradeBotError::Config(format!(
                    "config dir has conflicting watch config between `{}` and `{}`",
                    existing_source.display(),
                    path.display()
                )));
            }
        } else {
            merged.watch = Some(watch);
            *watch_source = Some(path.to_path_buf());
        }
    }

    for (name, template) in config.task_templates {
        if let Some(existing) = merged.task_templates.get(&name) {
            if existing != &template {
                return Err(TradeBotError::Config(format!(
                    "config dir has conflicting task template `{name}` in `{}`",
                    path.display()
                )));
            }
            continue;
        }
        merged.task_templates.insert(name, template);
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

fn watch_configs_equal(left: &WatchConfig, right: &WatchConfig) -> bool {
    left == right
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TaskListConfirmWindow {
    confirm_time: NaiveTime,
    first_task_time: NaiveTime,
}

fn task_list_confirm_is_due(
    config: &AppConfig,
    today: NaiveDate,
    weekday: Weekday,
    now_time: NaiveTime,
    previous_check: Option<(NaiveDate, NaiveTime)>,
    last_confirmed: Option<NaiveDate>,
) -> bool {
    if last_confirmed == Some(today) {
        return false;
    }

    let Some(window) = task_list_confirm_window(config, today, weekday) else {
        return false;
    };

    if now_time < window.confirm_time {
        return false;
    }

    if now_time <= window.first_task_time {
        return true;
    }

    previous_check.is_some_and(|(date, time)| date < today || time < window.confirm_time)
}

fn task_list_confirm_window(
    config: &AppConfig,
    today: NaiveDate,
    weekday: Weekday,
) -> Option<TaskListConfirmWindow> {
    let first_task_time = config
        .tasks
        .iter()
        .filter_map(|task| task.schedule.as_ref())
        .filter(|schedule| schedule_matches_day(schedule, today, weekday))
        .filter_map(|schedule| schedule.parse_time().ok())
        .min()?;

    let confirm_seconds = first_task_time
        .num_seconds_from_midnight()
        .saturating_sub(task_list_confirm_lead_seconds(config));
    let confirm_time = NaiveTime::from_num_seconds_from_midnight_opt(confirm_seconds, 0)
        .expect("seconds from midnight must build a valid time");

    Some(TaskListConfirmWindow {
        confirm_time,
        first_task_time,
    })
}

fn schedule_matches_day(schedule: &TaskScheduleConfig, today: NaiveDate, weekday: Weekday) -> bool {
    if let Ok(Some(date)) = schedule.parse_date() {
        if date != today {
            return false;
        }
    }

    schedule.is_enabled_on(weekday)
}

fn task_list_confirm_lead_seconds(config: &AppConfig) -> u32 {
    config
        .watch
        .task_list_confirm_lead_minutes
        .saturating_mul(60)
        .min(u32::MAX as u64) as u32
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
        task_list_confirm_is_due, task_list_confirm_window,
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
    fn task_list_confirm_tracks_earliest_enabled_task_for_today() {
        let today = NaiveDate::from_ymd_opt(2026, 3, 16).unwrap();
        let config = AppConfig::from_toml(
            r#"
[defaults]
timezone = "UTC"

[watch]
task_list_confirm_lead_minutes = 45

[brokers.paper]
kind = "ibkr"

[[tasks]]
name = "later"
broker = "paper"
action = "place"
schedule = { time = "10:15", weekdays = ["mon"] }
side = "buy"
pricing = { kind = "counterparty" }
shared_budget = { amount = 1000.0 }

[[tasks.symbols]]
ticker = "AAPL"
market = "us"
weight = 1.0

[[tasks]]
name = "open"
broker = "paper"
action = "place"
schedule = { time = "09:30", weekdays = ["mon"] }
side = "buy"
pricing = { kind = "counterparty" }
shared_budget = { amount = 1000.0 }

[[tasks.symbols]]
ticker = "MSFT"
market = "us"
weight = 1.0
"#,
        )
        .unwrap();

        let window = task_list_confirm_window(&config, today, Weekday::Mon).unwrap();
        assert_eq!(
            window.confirm_time,
            NaiveTime::from_hms_opt(8, 45, 0).unwrap()
        );
        assert_eq!(
            window.first_task_time,
            NaiveTime::from_hms_opt(9, 30, 0).unwrap()
        );
    }

    #[test]
    fn task_list_confirm_does_not_backfill_on_late_start_after_first_task() {
        let today = NaiveDate::from_ymd_opt(2026, 3, 16).unwrap();
        let config = AppConfig::from_toml(&sample_config("buy-aapl", "paper", "UTC")).unwrap();

        assert!(!task_list_confirm_is_due(
            &config,
            today,
            Weekday::Mon,
            NaiveTime::from_hms_opt(9, 45, 0).unwrap(),
            None,
            None,
        ));
    }

    #[test]
    fn task_list_confirm_sends_catch_up_if_watch_crosses_window() {
        let today = NaiveDate::from_ymd_opt(2026, 3, 16).unwrap();
        let config = AppConfig::from_toml(&sample_config("buy-aapl", "paper", "UTC")).unwrap();

        assert!(task_list_confirm_is_due(
            &config,
            today,
            Weekday::Mon,
            NaiveTime::from_hms_opt(9, 31, 0).unwrap(),
            Some((today, NaiveTime::from_hms_opt(8, 59, 0).unwrap())),
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
    fn load_config_dir_supports_shared_template_file() {
        let dir = temp_dir("template-file");
        let shared = r#"[defaults]
timezone = "UTC"
default_tif = "day"

[brokers.paper]
kind = "ibkr"

[task_templates.shared]
broker = "paper"
side = "buy"
pricing = { kind = "counterparty" }
shared_budget = { amount = 1000.0 }
"#;
        let task = r#"[[tasks]]
name = "task-a"
template = "shared"

[[tasks.symbols]]
ticker = "AAPL"
market = "us"
conid = "265598"
weight = 1.0
"#;
        fs::write(dir.join("shared.toml"), shared).unwrap();
        fs::write(dir.join("task.toml"), task).unwrap();

        let snapshot = load_config_dir(&dir).unwrap();
        assert_eq!(snapshot.config.tasks.len(), 1);
        assert_eq!(snapshot.config.tasks[0].broker, "paper");
        assert_eq!(
            snapshot.config.tasks[0].side,
            Some(trading_core::OrderSide::Buy)
        );
    }

    #[test]
    fn load_config_dir_allows_duplicate_identical_templates() {
        let dir = temp_dir("duplicate-template");
        let shared = r#"[defaults]
timezone = "UTC"
default_tif = "day"

[brokers.paper]
kind = "ibkr"

[task_templates.shared]
broker = "paper"
side = "buy"
pricing = { kind = "counterparty" }
shared_budget = { amount = 1000.0 }

[[tasks]]
name = "task-a"
template = "shared"

[[tasks.symbols]]
ticker = "AAPL"
market = "us"
conid = "265598"
weight = 1.0
"#;
        let duplicate_template = r#"[task_templates.shared]
broker = "paper"
side = "buy"
pricing = { kind = "counterparty" }
shared_budget = { amount = 1000.0 }

[[tasks]]
name = "task-b"
template = "shared"

[[tasks.symbols]]
ticker = "MSFT"
market = "us"
conid = "272093"
weight = 1.0
"#;
        fs::write(dir.join("a.toml"), shared).unwrap();
        fs::write(dir.join("b.toml"), duplicate_template).unwrap();

        let snapshot = load_config_dir(&dir).unwrap();
        assert_eq!(snapshot.config.tasks.len(), 2);
    }

    #[test]
    fn load_config_dir_rejects_conflicting_duplicate_templates() {
        let dir = temp_dir("conflicting-template");
        let shared = r#"[defaults]
timezone = "UTC"
default_tif = "day"

[brokers.paper]
kind = "ibkr"

[task_templates.shared]
broker = "paper"
side = "buy"
pricing = { kind = "counterparty" }
shared_budget = { amount = 1000.0 }

[[tasks]]
name = "task-a"
template = "shared"

[[tasks.symbols]]
ticker = "AAPL"
market = "us"
conid = "265598"
weight = 1.0
"#;
        let conflicting_template = r#"[task_templates.shared]
broker = "paper"
side = "sell"
pricing = { kind = "counterparty" }
shared_budget = { amount = 1000.0 }

[[tasks]]
name = "task-b"
template = "shared"

[[tasks.symbols]]
ticker = "MSFT"
market = "us"
conid = "272093"
weight = 1.0
"#;
        fs::write(dir.join("a.toml"), shared).unwrap();
        fs::write(dir.join("b.toml"), conflicting_template).unwrap();

        let err = load_config_dir(&dir).unwrap_err();
        assert!(
            err.to_string()
                .contains("conflicting task template `shared`")
        );
    }

    #[test]
    fn scheduled_task_descriptions_include_disabled_but_omit_manual_tasks() {
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
        assert_eq!(descriptions.len(), 2);
        assert!(descriptions[0].enabled);
        assert!(descriptions[0].description.contains("scheduled-a"));
        assert!(descriptions[0].description.contains("side=buy"));
        assert!(descriptions[0].description.contains("overdue=skip"));
        assert!(!descriptions[0].description.contains("action=place"));
        assert!(!descriptions[1].enabled);
        assert!(descriptions[1].description.contains("disabled-c"));
        assert!(descriptions[1].description.contains("side=buy"));
        assert!(!descriptions[1].description.contains("action=place"));
        assert!(!descriptions[1].description.contains("manual-b"));
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
