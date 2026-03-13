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
use trading_core::{AppConfig, Result, TaskConfig, TaskScheduleConfig, TradingEngine};

pub fn watch(config_path: PathBuf, poll_seconds: u64) -> Result<()> {
    let poll_interval = Duration::from_secs(poll_seconds.max(1));
    let mut state = WatchState::load(&config_path)?;

    info!(
        config = %config_path.display(),
        timezone = %state.schedule_timezone(),
        poll_seconds = poll_interval.as_secs(),
        "watching config for scheduled tasks"
    );

    loop {
        state.reload_if_changed(&config_path);
        state.run_due_tasks();
        thread::sleep(poll_interval);
    }
}

struct WatchState {
    config: AppConfig,
    engine: TradingEngine,
    last_seen_raw: String,
    last_attempted: BTreeMap<String, NaiveDate>,
}

impl WatchState {
    fn load(config_path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(config_path)?;
        let config = AppConfig::from_toml(&raw)?;

        Ok(Self {
            engine: crate::build_engine(config.clone()),
            config,
            last_seen_raw: raw,
            last_attempted: BTreeMap::new(),
        })
    }

    fn reload_if_changed(&mut self, config_path: &Path) {
        let raw = match fs::read_to_string(config_path) {
            Ok(raw) => raw,
            Err(err) => {
                warn!(
                    config = %config_path.display(),
                    error = %err,
                    "failed to read config; keeping previous valid config"
                );
                return;
            }
        };

        if raw == self.last_seen_raw {
            return;
        }

        self.last_seen_raw = raw.clone();
        match AppConfig::from_toml(&raw) {
            Ok(config) => {
                self.last_attempted
                    .retain(|task_name, _| config.tasks.iter().any(|task| task.name == *task_name));
                self.engine = crate::build_engine(config.clone());
                self.config = config;
                info!(
                    config = %config_path.display(),
                    timezone = %self.schedule_timezone(),
                    "reloaded config and applied updated schedules"
                );
            }
            Err(err) => {
                warn!(
                    config = %config_path.display(),
                    error = %err,
                    "config reload rejected; keeping previous valid config"
                );
            }
        }
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
    use chrono::{NaiveDate, NaiveTime, Weekday};
    use trading_core::{ScheduleWeekday, TaskScheduleConfig};

    use super::schedule_is_due;

    fn weekday_only_schedule() -> TaskScheduleConfig {
        TaskScheduleConfig {
            time: "09:30".into(),
            weekdays: vec![ScheduleWeekday::Mon, ScheduleWeekday::Tue],
            enabled: true,
        }
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
}
