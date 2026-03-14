use std::{collections::BTreeMap, fs, path::Path};

use chrono::{NaiveTime, Weekday};
use chrono_tz::Tz;
use serde::Deserialize;

use crate::{
    errors::{Result, TradeBotError},
    models::{
        ExecutionPolicy, OrderSide, PricingSpec, RiskPolicy, SessionPolicy, SharedBudget,
        SymbolTarget, TaskAction, TimeInForce,
    },
};

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub defaults: DefaultsConfig,
    pub brokers: BTreeMap<String, BrokerConfig>,
    pub tasks: Vec<TaskConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DefaultsConfig {
    #[serde(default = "default_timezone")]
    pub timezone: String,
    #[serde(default = "default_tif")]
    pub default_tif: TimeInForce,
    #[serde(default)]
    pub email: Option<EmailTransportConfig>,
}

impl Default for DefaultsConfig {
    fn default() -> Self {
        Self {
            timezone: default_timezone(),
            default_tif: default_tif(),
            email: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct EmailTransportConfig {
    #[serde(default)]
    pub subject_prefix: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BrokerConfig {
    pub kind: String,
    #[serde(default)]
    pub env_prefix: Option<String>,
    #[serde(flatten)]
    pub settings: BTreeMap<String, toml::Value>,
}

impl BrokerConfig {
    pub fn env_var_name(&self, default_name: &str, suffix: &str) -> String {
        self.env_prefix
            .as_deref()
            .map(str::trim)
            .filter(|prefix| !prefix.is_empty())
            .map(|prefix| format!("{}_{}", prefix.trim_end_matches('_'), suffix))
            .unwrap_or_else(|| default_name.to_string())
    }

    fn validate(&self, broker_name: &str) -> Result<()> {
        if matches!(self.env_prefix.as_deref(), Some(raw) if raw.trim().is_empty()) {
            return Err(TradeBotError::Config(format!(
                "broker `{broker_name}` env_prefix cannot be empty"
            )));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaskConfig {
    pub name: String,
    pub broker: String,
    pub action: TaskAction,
    #[serde(default)]
    pub note: Option<String>,
    #[serde(default)]
    pub schedule: Option<TaskScheduleConfig>,
    #[serde(default)]
    pub execution: Option<ExecutionPolicy>,
    #[serde(default)]
    pub notify: Option<TaskNotificationConfig>,
    #[serde(default)]
    pub side: Option<OrderSide>,
    #[serde(default)]
    pub pricing: Option<PricingSpec>,
    #[serde(default)]
    pub risk: Option<RiskPolicy>,
    #[serde(default)]
    pub session: Option<SessionPolicy>,
    #[serde(default)]
    pub shared_budget: Option<SharedBudget>,
    #[serde(default)]
    pub time_in_force: Option<TimeInForce>,
    #[serde(default)]
    pub client_tag: Option<String>,
    #[serde(default)]
    pub all_open: bool,
    #[serde(default)]
    pub symbols: Vec<SymbolTarget>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaskNotificationConfig {
    #[serde(default)]
    pub email: Option<EmailNotificationConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EmailNotificationConfig {
    pub to: Vec<String>,
    #[serde(default = "default_notification_events")]
    pub on: Vec<NotificationEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationEvent {
    Success,
    Failure,
    Filled,
    PartialFilled,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaskScheduleConfig {
    #[serde(default)]
    pub date: Option<String>,
    pub time: String,
    #[serde(default)]
    pub weekdays: Vec<ScheduleWeekday>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "default_overdue_policy")]
    pub overdue_policy: ScheduleOverduePolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScheduleWeekday {
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
    Sun,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScheduleOverduePolicy {
    Run,
    Skip,
}

impl AppConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let raw = fs::read_to_string(path)?;
        Self::from_toml(&raw)
    }

    pub fn from_toml(raw: &str) -> Result<Self> {
        let parsed: Self = toml::from_str(raw)?;
        parsed.validate()?;
        Ok(parsed)
    }

    pub fn validate(&self) -> Result<()> {
        self.defaults.parse_timezone()?;

        if self.brokers.is_empty() {
            return Err(TradeBotError::Config(
                "at least one broker is required".into(),
            ));
        }

        for (broker_name, broker) in &self.brokers {
            broker.validate(broker_name)?;
        }

        if self.tasks.is_empty() {
            return Err(TradeBotError::Config(
                "at least one task is required".into(),
            ));
        }

        let mut names = std::collections::BTreeSet::new();
        for task in &self.tasks {
            if !names.insert(task.name.clone()) {
                return Err(TradeBotError::Config(format!(
                    "duplicate task name `{}`",
                    task.name
                )));
            }
            self.validate_task(task)?;
        }
        Ok(())
    }

    pub fn task(&self, name: &str) -> Result<&TaskConfig> {
        self.tasks
            .iter()
            .find(|task| task.name == name)
            .ok_or_else(|| TradeBotError::NotFound(format!("task `{name}`")))
    }

    pub fn broker(&self, name: &str) -> Result<&BrokerConfig> {
        self.brokers
            .get(name)
            .ok_or_else(|| TradeBotError::NotFound(format!("broker `{name}`")))
    }

    fn validate_task(&self, task: &TaskConfig) -> Result<()> {
        if !self.brokers.contains_key(&task.broker) {
            return Err(TradeBotError::Config(format!(
                "task `{}` references unknown broker `{}`",
                task.name, task.broker
            )));
        }

        if let Some(schedule) = &task.schedule {
            schedule.validate(&task.name)?;
        }

        if let Some(execution) = &task.execution {
            validate_execution_policy(&task.name, task.action, execution)?;
        }

        if let Some(notify) = &task.notify {
            notify.validate(&self.defaults, &task.name)?;
        }

        if task.symbols.is_empty() && !task.all_open {
            return Err(TradeBotError::Validation(format!(
                "task `{}` must declare symbols unless all_open = true",
                task.name
            )));
        }

        match task.action {
            TaskAction::Place => self.validate_place_task(task),
            TaskAction::Cancel => self.validate_cancel_task(task),
        }
    }

    fn validate_place_task(&self, task: &TaskConfig) -> Result<()> {
        if task.side.is_none() {
            return Err(TradeBotError::Validation(format!(
                "task `{}` requires side for place action",
                task.name
            )));
        }

        let pricing = task.pricing.as_ref().ok_or_else(|| {
            TradeBotError::Validation(format!(
                "task `{}` requires pricing for place action",
                task.name
            ))
        })?;

        let mut has_explicit_allocation = false;
        let mut has_close_position = false;
        let mut weight_sum = 0.0;

        for symbol in &task.symbols {
            if symbol.close_position {
                has_close_position = true;
                if symbol.quantity.is_some()
                    || symbol.amount.is_some()
                    || symbol.weight.is_some()
                    || symbol.min_quantity.is_some()
                    || symbol.quantity_step.is_some()
                {
                    return Err(TradeBotError::Validation(format!(
                        "task `{}` symbol `{}` cannot combine close_position with quantity/amount/weight/min_quantity/quantity_step",
                        task.name, symbol.instrument.ticker
                    )));
                }
            }
            validate_symbol_trade_constraints(&task.name, symbol)?;
            if symbol.quantity.is_some() && symbol.amount.is_some() {
                return Err(TradeBotError::Validation(format!(
                    "task `{}` symbol `{}` cannot set both quantity and amount",
                    task.name, symbol.instrument.ticker
                )));
            }
            if symbol.quantity.is_some() || symbol.amount.is_some() {
                has_explicit_allocation = true;
            }
            if let Some(weight) = symbol.weight {
                if weight <= 0.0 {
                    return Err(TradeBotError::Validation(format!(
                        "task `{}` symbol `{}` weight must be positive",
                        task.name, symbol.instrument.ticker
                    )));
                }
                weight_sum += weight;
            }
            if matches!(pricing, PricingSpec::Limit { .. }) {
                let global_limit = match pricing {
                    PricingSpec::Limit { price } => *price,
                    _ => None,
                };
                if global_limit.is_none() && symbol.limit_price.is_none() {
                    return Err(TradeBotError::Validation(format!(
                        "task `{}` symbol `{}` needs a limit price",
                        task.name, symbol.instrument.ticker
                    )));
                }
            }
        }

        if has_close_position && task.side != Some(OrderSide::Sell) {
            return Err(TradeBotError::Validation(format!(
                "task `{}` close_position requires side = \"sell\"",
                task.name
            )));
        }

        if let Some(shared_budget) = &task.shared_budget {
            if shared_budget.amount <= 0.0 {
                return Err(TradeBotError::Validation(format!(
                    "task `{}` shared_budget.amount must be positive",
                    task.name
                )));
            }
            if has_explicit_allocation {
                return Err(TradeBotError::Validation(format!(
                    "task `{}` cannot combine shared_budget with per-symbol quantity/amount",
                    task.name
                )));
            }
            if has_close_position {
                return Err(TradeBotError::Validation(format!(
                    "task `{}` cannot combine shared_budget with close_position symbols",
                    task.name
                )));
            }
            if (weight_sum - 0.0).abs() < f64::EPSILON {
                return Err(TradeBotError::Validation(format!(
                    "task `{}` shared_budget requires symbol weights",
                    task.name
                )));
            }
        } else if !has_explicit_allocation && !has_close_position {
            return Err(TradeBotError::Validation(format!(
                "task `{}` requires quantity/amount, close_position, or shared_budget",
                task.name
            )));
        }

        Ok(())
    }

    fn validate_cancel_task(&self, task: &TaskConfig) -> Result<()> {
        if task.pricing.is_some() || task.shared_budget.is_some() {
            return Err(TradeBotError::Validation(format!(
                "task `{}` cancel action cannot define pricing/shared_budget",
                task.name
            )));
        }
        if task.session.is_some() {
            return Err(TradeBotError::Validation(format!(
                "task `{}` cancel action cannot define session",
                task.name
            )));
        }
        Ok(())
    }
}

fn default_tif() -> TimeInForce {
    TimeInForce::Day
}

fn default_timezone() -> String {
    "UTC".to_string()
}

fn default_enabled() -> bool {
    true
}

fn default_overdue_policy() -> ScheduleOverduePolicy {
    ScheduleOverduePolicy::Run
}

fn default_notification_events() -> Vec<NotificationEvent> {
    vec![NotificationEvent::Success]
}

fn validate_symbol_trade_constraints(task_name: &str, symbol: &SymbolTarget) -> Result<()> {
    if matches!(symbol.min_quantity, Some(0)) {
        return Err(TradeBotError::Validation(format!(
            "task `{task_name}` symbol `{}` min_quantity must be positive",
            symbol.instrument.ticker
        )));
    }
    if matches!(symbol.quantity_step, Some(0)) {
        return Err(TradeBotError::Validation(format!(
            "task `{task_name}` symbol `{}` quantity_step must be positive",
            symbol.instrument.ticker
        )));
    }

    if let Some(quantity) = symbol.quantity {
        let quantity_step = symbol.quantity_step.unwrap_or(1);
        let min_quantity = align_up_to_step(symbol.min_quantity.unwrap_or(1), quantity_step)?;
        if quantity < min_quantity {
            return Err(TradeBotError::Validation(format!(
                "task `{task_name}` symbol `{}` quantity must be at least {min_quantity}",
                symbol.instrument.ticker
            )));
        }
        if quantity % quantity_step != 0 {
            return Err(TradeBotError::Validation(format!(
                "task `{task_name}` symbol `{}` quantity must align to quantity_step {quantity_step}",
                symbol.instrument.ticker
            )));
        }
    }

    Ok(())
}

fn align_up_to_step(quantity: u64, quantity_step: u64) -> Result<u64> {
    let remainder = quantity % quantity_step;
    if remainder == 0 {
        return Ok(quantity);
    }

    quantity
        .checked_add(quantity_step - remainder)
        .ok_or_else(|| {
            TradeBotError::Validation(
                "quantity constraint overflow while aligning min_quantity to quantity_step".into(),
            )
        })
}

fn validate_execution_policy(
    task_name: &str,
    action: TaskAction,
    execution: &ExecutionPolicy,
) -> Result<()> {
    if action != TaskAction::Place {
        return Err(TradeBotError::Validation(format!(
            "task `{task_name}` execution policy is only supported for place action"
        )));
    }

    match execution {
        ExecutionPolicy::OneShot => {}
        ExecutionPolicy::Track {
            timeout_seconds,
            poll_seconds,
        } => {
            if *timeout_seconds == 0 {
                return Err(TradeBotError::Validation(format!(
                    "task `{task_name}` execution.track.timeout_seconds must be positive"
                )));
            }
            if *poll_seconds == 0 {
                return Err(TradeBotError::Validation(format!(
                    "task `{task_name}` execution.poll_seconds must be positive"
                )));
            }
        }
        ExecutionPolicy::CancelReplace {
            timeout_seconds,
            poll_seconds,
            max_attempts,
        } => {
            if *poll_seconds == 0 {
                return Err(TradeBotError::Validation(format!(
                    "task `{task_name}` execution.poll_seconds must be positive"
                )));
            }
            if *max_attempts == 0 {
                return Err(TradeBotError::Validation(format!(
                    "task `{task_name}` execution.max_attempts must be at least 1"
                )));
            }
            if *timeout_seconds == 0 && *max_attempts == 1 {
                return Err(TradeBotError::Validation(format!(
                    "task `{task_name}` execution.cancel_replace needs timeout_seconds > 0 or max_attempts > 1"
                )));
            }
        }
    }

    Ok(())
}

impl DefaultsConfig {
    pub fn parse_timezone(&self) -> Result<Tz> {
        self.timezone.parse::<Tz>().map_err(|_| {
            TradeBotError::Config(format!(
                "invalid defaults.timezone `{}`; expected an IANA timezone like `UTC` or `Asia/Shanghai`",
                self.timezone
            ))
        })
    }
}

impl TaskScheduleConfig {
    pub fn parse_date(&self) -> Result<Option<chrono::NaiveDate>> {
        self.date
            .as_deref()
            .map(|raw| {
                chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d").map_err(|_| {
                    TradeBotError::Config(format!(
                        "invalid schedule date `{raw}`; expected YYYY-MM-DD"
                    ))
                })
            })
            .transpose()
    }

    pub fn parse_time(&self) -> Result<NaiveTime> {
        parse_schedule_time(&self.time).ok_or_else(|| {
            TradeBotError::Config(format!(
                "invalid schedule time `{}`; expected HH:MM or HH:MM:SS",
                self.time
            ))
        })
    }

    pub fn is_enabled_on(&self, weekday: Weekday) -> bool {
        self.enabled
            && (self.weekdays.is_empty()
                || self
                    .weekdays
                    .iter()
                    .copied()
                    .any(|configured| configured.to_chrono() == weekday))
    }

    fn validate(&self, task_name: &str) -> Result<()> {
        self.parse_date().map_err(|err| match err {
            TradeBotError::Config(message) => {
                TradeBotError::Config(format!("task `{task_name}` schedule {message}"))
            }
            other => other,
        })?;
        self.parse_time().map_err(|err| match err {
            TradeBotError::Config(message) => {
                TradeBotError::Config(format!("task `{task_name}` schedule {message}"))
            }
            other => other,
        })?;

        let mut seen = std::collections::BTreeSet::new();
        for weekday in &self.weekdays {
            if !seen.insert(*weekday) {
                return Err(TradeBotError::Config(format!(
                    "task `{task_name}` schedule has duplicate weekday `{}`",
                    weekday.as_str()
                )));
            }
        }
        Ok(())
    }
}

impl ScheduleWeekday {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mon => "mon",
            Self::Tue => "tue",
            Self::Wed => "wed",
            Self::Thu => "thu",
            Self::Fri => "fri",
            Self::Sat => "sat",
            Self::Sun => "sun",
        }
    }

    pub fn to_chrono(self) -> Weekday {
        match self {
            Self::Mon => Weekday::Mon,
            Self::Tue => Weekday::Tue,
            Self::Wed => Weekday::Wed,
            Self::Thu => Weekday::Thu,
            Self::Fri => Weekday::Fri,
            Self::Sat => Weekday::Sat,
            Self::Sun => Weekday::Sun,
        }
    }
}

impl TaskNotificationConfig {
    fn validate(&self, _defaults: &DefaultsConfig, task_name: &str) -> Result<()> {
        let Some(email) = &self.email else {
            return Ok(());
        };

        if email.to.is_empty() {
            return Err(TradeBotError::Config(format!(
                "task `{task_name}` notify.email.to must contain at least one recipient"
            )));
        }

        for recipient in &email.to {
            if !looks_like_email(recipient) {
                return Err(TradeBotError::Config(format!(
                    "task `{task_name}` notify.email recipient `{recipient}` is not a valid email address"
                )));
            }
        }

        let mut seen = std::collections::BTreeSet::new();
        for event in &email.on {
            if !seen.insert(*event) {
                return Err(TradeBotError::Config(format!(
                    "task `{task_name}` notify.email has duplicate event"
                )));
            }
        }

        Ok(())
    }
}

fn looks_like_email(raw: &str) -> bool {
    let trimmed = raw.trim();
    !trimmed.is_empty() && trimmed.contains('@')
}

fn parse_schedule_time(raw: &str) -> Option<NaiveTime> {
    const FORMATS: [&str; 2] = ["%H:%M", "%H:%M:%S"];

    FORMATS
        .iter()
        .find_map(|format| NaiveTime::parse_from_str(raw, format).ok())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        AppConfig, BrokerConfig, DefaultsConfig, EmailNotificationConfig, NotificationEvent,
        ScheduleOverduePolicy, TaskConfig, TaskNotificationConfig, TaskScheduleConfig,
    };
    use crate::{
        errors::TradeBotError,
        models::{
            ExecutionPolicy, InstrumentRef, Market, OrderSide, PricingSpec, SessionPolicy,
            SharedBudget, SymbolTarget, TaskAction,
        },
    };

    fn sample_task() -> TaskConfig {
        TaskConfig {
            name: "scheduled-rebalance".into(),
            broker: "paper".into(),
            action: TaskAction::Place,
            note: None,
            schedule: Some(TaskScheduleConfig {
                date: None,
                time: "09:30".into(),
                weekdays: Vec::new(),
                enabled: true,
                overdue_policy: ScheduleOverduePolicy::Run,
            }),
            execution: None,
            notify: None,
            side: Some(OrderSide::Buy),
            pricing: Some(PricingSpec::Counterparty),
            risk: None,
            session: None,
            shared_budget: Some(SharedBudget { amount: 1000.0 }),
            time_in_force: None,
            client_tag: None,
            all_open: false,
            symbols: vec![SymbolTarget {
                instrument: InstrumentRef {
                    ticker: "AAPL".into(),
                    market: Market::Us,
                    broker_symbol: None,
                    conid: None,
                },
                close_position: false,
                quantity: None,
                amount: None,
                weight: Some(1.0),
                min_quantity: None,
                quantity_step: None,
                limit_price: None,
                client_order_id: None,
                broker_options: BTreeMap::new(),
            }],
        }
    }

    fn sample_config(task: TaskConfig) -> AppConfig {
        AppConfig {
            defaults: DefaultsConfig::default(),
            brokers: BTreeMap::from([(
                "paper".into(),
                BrokerConfig {
                    kind: "mock".into(),
                    env_prefix: None,
                    settings: BTreeMap::new(),
                },
            )]),
            tasks: vec![task],
        }
    }

    #[test]
    fn accepts_schedule_with_hours_and_minutes() {
        let config = sample_config(sample_task());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn accepts_task_email_notification_without_transport() {
        let mut task = sample_task();
        task.notify = Some(TaskNotificationConfig {
            email: Some(EmailNotificationConfig {
                to: vec!["ops@example.com".into()],
                on: vec![NotificationEvent::Success, NotificationEvent::Failure],
            }),
        });

        assert!(sample_config(task).validate().is_ok());
    }

    #[test]
    fn rejects_invalid_timezone() {
        let mut config = sample_config(sample_task());
        config.defaults.timezone = "Mars/Olympus".into();

        let err = config.validate().unwrap_err();
        assert!(
            matches!(err, TradeBotError::Config(message) if message.contains("invalid defaults.timezone"))
        );
    }

    #[test]
    fn rejects_empty_broker_env_prefix() {
        let mut config = sample_config(sample_task());
        config
            .brokers
            .get_mut("paper")
            .expect("paper broker")
            .env_prefix = Some("   ".into());

        let err = config.validate().unwrap_err();
        assert!(
            matches!(err, TradeBotError::Config(message) if message.contains("env_prefix cannot be empty"))
        );
    }

    #[test]
    fn rejects_invalid_schedule_time() {
        let mut task = sample_task();
        task.schedule.as_mut().unwrap().time = "25:99".into();

        let err = sample_config(task).validate().unwrap_err();
        assert!(
            matches!(err, TradeBotError::Config(message) if message.contains("invalid schedule time"))
        );
    }

    #[test]
    fn rejects_duplicate_schedule_weekdays() {
        let mut task = sample_task();
        task.schedule.as_mut().unwrap().weekdays =
            vec![super::ScheduleWeekday::Mon, super::ScheduleWeekday::Mon];

        let err = sample_config(task).validate().unwrap_err();
        assert!(
            matches!(err, TradeBotError::Config(message) if message.contains("duplicate weekday"))
        );
    }

    #[test]
    fn accepts_skip_overdue_policy() {
        let mut task = sample_task();
        task.schedule.as_mut().unwrap().overdue_policy = ScheduleOverduePolicy::Skip;

        assert!(sample_config(task).validate().is_ok());
    }

    #[test]
    fn accepts_one_shot_schedule_date() {
        let mut task = sample_task();
        task.schedule.as_mut().unwrap().date = Some("2026-03-13".into());

        let config = sample_config(task);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn rejects_invalid_schedule_date() {
        let mut task = sample_task();
        task.schedule.as_mut().unwrap().date = Some("2026/03/13".into());

        let err = sample_config(task).validate().unwrap_err();
        assert!(
            matches!(err, TradeBotError::Config(message) if message.contains("invalid schedule date"))
        );
    }

    #[test]
    fn rejects_close_position_with_shared_budget() {
        let mut task = sample_task();
        task.side = Some(OrderSide::Sell);
        task.symbols[0].close_position = true;
        task.symbols[0].weight = None;

        let err = sample_config(task).validate().unwrap_err();
        assert!(
            matches!(err, TradeBotError::Validation(message) if message.contains("cannot combine shared_budget with close_position"))
        );
    }

    #[test]
    fn rejects_zero_quantity_step() {
        let mut task = sample_task();
        task.symbols[0].quantity_step = Some(0);

        let err = sample_config(task).validate().unwrap_err();
        assert!(
            matches!(err, TradeBotError::Validation(message) if message.contains("quantity_step must be positive"))
        );
    }

    #[test]
    fn rejects_explicit_quantity_that_does_not_align_to_step() {
        let mut task = sample_task();
        task.shared_budget = None;
        task.symbols[0].weight = None;
        task.symbols[0].quantity = Some(150);
        task.symbols[0].min_quantity = Some(100);
        task.symbols[0].quantity_step = Some(100);

        let err = sample_config(task).validate().unwrap_err();
        assert!(
            matches!(err, TradeBotError::Validation(message) if message.contains("quantity must align to quantity_step 100"))
        );
    }

    #[test]
    fn rejects_execution_policy_for_cancel_task() {
        let mut task = sample_task();
        task.action = TaskAction::Cancel;
        task.pricing = None;
        task.shared_budget = None;
        task.execution = Some(ExecutionPolicy::OneShot);

        let err = sample_config(task).validate().unwrap_err();
        assert!(
            matches!(err, TradeBotError::Validation(message) if message.contains("execution policy is only supported for place action"))
        );
    }

    #[test]
    fn accepts_explicit_one_shot_execution() {
        let mut task = sample_task();
        task.execution = Some(ExecutionPolicy::OneShot);

        assert!(sample_config(task).validate().is_ok());
    }

    #[test]
    fn rejects_track_execution_without_timeout() {
        let mut task = sample_task();
        task.execution = Some(ExecutionPolicy::Track {
            timeout_seconds: 0,
            poll_seconds: 5,
        });

        let err = sample_config(task).validate().unwrap_err();
        assert!(
            matches!(err, TradeBotError::Validation(message) if message.contains("execution.track.timeout_seconds must be positive"))
        );
    }

    #[test]
    fn rejects_session_for_cancel_task() {
        let mut task = sample_task();
        task.action = TaskAction::Cancel;
        task.pricing = None;
        task.shared_budget = None;
        task.session = Some(SessionPolicy {
            extended_hours: true,
        });

        let err = sample_config(task).validate().unwrap_err();
        assert!(
            matches!(err, TradeBotError::Validation(message) if message.contains("cancel action cannot define session"))
        );
    }
}
