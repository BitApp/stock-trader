use std::{collections::BTreeMap, fs, path::Path};

use chrono::{NaiveTime, Weekday};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

use crate::{
    errors::{Result, TradeBotError},
    models::{
        ExecutionPolicy, OrderSide, PricingSpec, RiskPolicy, SessionPolicy, SharedBudget,
        SymbolTarget, TaskAction, TimeInForce,
    },
};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AppConfig {
    pub defaults: DefaultsConfig,
    pub watch: WatchConfig,
    pub brokers: BTreeMap<String, BrokerConfig>,
    pub tasks: Vec<TaskConfig>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmailTransportConfig {
    #[serde(default)]
    pub subject_prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TaskConfig {
    pub name: String,
    pub broker: String,
    #[serde(default = "default_task_action")]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WatchConfig {
    #[serde(default = "default_reload_debounce_seconds")]
    pub reload_debounce_seconds: u64,
    #[serde(default)]
    pub notify: Option<WatchNotificationConfig>,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            reload_debounce_seconds: default_reload_debounce_seconds(),
            notify: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WatchNotificationConfig {
    #[serde(default)]
    pub email: Option<WatchEmailNotificationConfig>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WatchEmailNotificationConfig {
    pub to: Vec<String>,
    #[serde(default = "default_watch_notification_events")]
    pub on: Vec<WatchNotificationEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchNotificationEvent {
    TaskListLoaded,
    TaskListChanged,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskNotificationConfig {
    #[serde(default)]
    pub email: Option<EmailNotificationConfig>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmailNotificationConfig {
    pub to: Vec<String>,
    #[serde(default = "default_notification_events")]
    pub on: Vec<NotificationEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationEvent {
    Success,
    Failure,
    Filled,
    PartialFilled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScheduleOverduePolicy {
    Run,
    Skip,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct ConfigFragment {
    #[serde(default)]
    pub defaults: Option<DefaultsConfig>,
    #[serde(default)]
    pub watch: Option<WatchConfig>,
    #[serde(default)]
    pub brokers: BTreeMap<String, BrokerConfig>,
    #[serde(default)]
    pub task_templates: BTreeMap<String, TaskFieldsConfig>,
    #[serde(default)]
    pub tasks: Vec<RawTaskConfig>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct RawTaskConfig {
    pub name: String,
    #[serde(default)]
    pub template: Option<String>,
    #[serde(flatten)]
    pub fields: TaskFieldsConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct TaskFieldsConfig {
    #[serde(default)]
    pub broker: Option<String>,
    #[serde(default)]
    pub action: Option<TaskAction>,
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
    pub all_open: Option<bool>,
    #[serde(default)]
    pub symbols: Option<Vec<SymbolTarget>>,
}

impl AppConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let raw = fs::read_to_string(path)?;
        Self::from_toml(&raw)
    }

    pub fn from_toml(raw: &str) -> Result<Self> {
        let fragment = ConfigFragment::from_toml(raw)?;
        Self::from_fragment(fragment)
    }

    pub fn from_fragment(fragment: ConfigFragment) -> Result<Self> {
        fragment.into_app_config()
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

        self.watch.validate(&self.defaults)?;

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

impl ConfigFragment {
    pub fn from_toml(raw: &str) -> Result<Self> {
        Ok(toml::from_str(raw)?)
    }

    pub fn into_app_config(self) -> Result<AppConfig> {
        let defaults = self.defaults.unwrap_or_default();
        let watch = self.watch.unwrap_or_default();
        let templates = self.task_templates;
        let tasks = self
            .tasks
            .into_iter()
            .map(|task| task.expand(&templates))
            .collect::<Result<Vec<_>>>()?;
        let config = AppConfig {
            defaults,
            watch,
            brokers: self.brokers,
            tasks,
        };
        config.validate()?;
        Ok(config)
    }
}

fn default_tif() -> TimeInForce {
    TimeInForce::Day
}

fn default_task_action() -> TaskAction {
    TaskAction::Place
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

fn default_watch_notification_events() -> Vec<WatchNotificationEvent> {
    vec![
        WatchNotificationEvent::TaskListLoaded,
        WatchNotificationEvent::TaskListChanged,
    ]
}

fn default_reload_debounce_seconds() -> u64 {
    600
}

impl RawTaskConfig {
    fn expand(self, templates: &BTreeMap<String, TaskFieldsConfig>) -> Result<TaskConfig> {
        let mut fields = self
            .template
            .as_deref()
            .map(|template_name| {
                templates.get(template_name).cloned().ok_or_else(|| {
                    TradeBotError::Config(format!(
                        "task `{}` references unknown template `{template_name}`",
                        self.name
                    ))
                })
            })
            .transpose()?
            .unwrap_or_default();
        fields.apply_overrides(self.fields);
        fields.into_task(self.name)
    }
}

impl TaskFieldsConfig {
    fn apply_overrides(&mut self, overrides: TaskFieldsConfig) {
        if let Some(value) = overrides.broker {
            self.broker = Some(value);
        }
        if let Some(value) = overrides.action {
            self.action = Some(value);
        }
        if let Some(value) = overrides.note {
            self.note = Some(value);
        }
        if let Some(value) = overrides.schedule {
            self.schedule = Some(value);
        }
        if let Some(value) = overrides.execution {
            self.execution = Some(value);
        }
        if let Some(value) = overrides.notify {
            self.notify = Some(value);
        }
        if let Some(value) = overrides.side {
            self.side = Some(value);
        }
        if let Some(value) = overrides.pricing {
            self.pricing = Some(value);
        }
        if let Some(value) = overrides.risk {
            self.risk = Some(value);
        }
        if let Some(value) = overrides.session {
            self.session = Some(value);
        }
        if let Some(value) = overrides.shared_budget {
            self.shared_budget = Some(value);
        }
        if let Some(value) = overrides.time_in_force {
            self.time_in_force = Some(value);
        }
        if let Some(value) = overrides.client_tag {
            self.client_tag = Some(value);
        }
        if let Some(value) = overrides.all_open {
            self.all_open = Some(value);
        }
        if let Some(value) = overrides.symbols {
            self.symbols = Some(value);
        }
    }

    fn into_task(self, name: String) -> Result<TaskConfig> {
        let broker = self.broker.ok_or_else(|| {
            TradeBotError::Config(format!("task `{name}` requires broker or template broker"))
        })?;

        Ok(TaskConfig {
            name,
            broker,
            action: self.action.unwrap_or(default_task_action()),
            note: self.note,
            schedule: self.schedule,
            execution: self.execution,
            notify: self.notify,
            side: self.side,
            pricing: self.pricing,
            risk: self.risk,
            session: self.session,
            shared_budget: self.shared_budget,
            time_in_force: self.time_in_force,
            client_tag: self.client_tag,
            all_open: self.all_open.unwrap_or(false),
            symbols: self.symbols.unwrap_or_default(),
        })
    }
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

impl WatchConfig {
    fn validate(&self, _defaults: &DefaultsConfig) -> Result<()> {
        if self.reload_debounce_seconds > 24 * 60 * 60 {
            return Err(TradeBotError::Config(
                "watch.reload_debounce_seconds must be at most 86400".into(),
            ));
        }

        let Some(notify) = &self.notify else {
            return Ok(());
        };
        notify.validate()
    }
}

impl WatchNotificationConfig {
    fn validate(&self) -> Result<()> {
        let Some(email) = &self.email else {
            return Ok(());
        };

        if email.to.is_empty() {
            return Err(TradeBotError::Config(
                "watch.notify.email.to must contain at least one recipient".into(),
            ));
        }

        for recipient in &email.to {
            if !looks_like_email(recipient) {
                return Err(TradeBotError::Config(format!(
                    "watch.notify.email recipient `{recipient}` is not a valid email address"
                )));
            }
        }

        let mut seen = std::collections::BTreeSet::new();
        for event in &email.on {
            if !seen.insert(*event) {
                return Err(TradeBotError::Config(format!(
                    "watch.notify.email has duplicate event `{}`",
                    watch_notification_event_as_str(*event)
                )));
            }
        }

        Ok(())
    }
}

fn watch_notification_event_as_str(event: WatchNotificationEvent) -> &'static str {
    match event {
        WatchNotificationEvent::TaskListLoaded => "task_list_loaded",
        WatchNotificationEvent::TaskListChanged => "task_list_changed",
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
        ScheduleOverduePolicy, TaskConfig, TaskNotificationConfig, TaskScheduleConfig, WatchConfig,
        WatchEmailNotificationConfig, WatchNotificationConfig, WatchNotificationEvent,
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
            watch: WatchConfig::default(),
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
    fn defaults_missing_task_action_to_place() {
        let raw = r#"[defaults]
timezone = "UTC"

[brokers.paper]
kind = "ibkr"

[[tasks]]
name = "buy-aapl"
broker = "paper"
side = "buy"
pricing = { kind = "counterparty" }
shared_budget = { amount = 1000.0 }

[[tasks.symbols]]
ticker = "AAPL"
market = "us"
weight = 1.0
"#;

        let config = AppConfig::from_toml(raw).unwrap();
        assert_eq!(config.tasks[0].action, TaskAction::Place);
    }

    #[test]
    fn task_template_populates_shared_fields() {
        let raw = r#"[defaults]
timezone = "UTC"

[brokers.paper]
kind = "ibkr"

[task_templates.swing]
broker = "paper"
client_tag = "template-tag"
side = "buy"
pricing = { kind = "counterparty" }
shared_budget = { amount = 1000.0 }
notify = { email = { to = ["ops@example.com"], on = ["success", "failure"] } }

[[tasks]]
name = "buy-aapl"
template = "swing"

[[tasks.symbols]]
ticker = "AAPL"
market = "us"
weight = 1.0
"#;

        let config = AppConfig::from_toml(raw).unwrap();
        let task = &config.tasks[0];
        assert_eq!(task.broker, "paper");
        assert_eq!(task.client_tag.as_deref(), Some("template-tag"));
        assert_eq!(task.side, Some(OrderSide::Buy));
        assert!(matches!(task.pricing, Some(PricingSpec::Counterparty)));
        assert!(task.notify.is_some());
    }

    #[test]
    fn task_fields_override_template_and_replace_arrays() {
        let raw = r#"[defaults]
timezone = "UTC"

[brokers.paper]
kind = "ibkr"

[task_templates.base]
broker = "paper"
side = "buy"
pricing = { kind = "counterparty" }
shared_budget = { amount = 1000.0 }

[[task_templates.base.symbols]]
ticker = "AAPL"
market = "us"
weight = 1.0

[[tasks]]
name = "buy-msft"
template = "base"
side = "sell"

[[tasks.symbols]]
ticker = "MSFT"
market = "us"
weight = 1.0
"#;

        let config = AppConfig::from_toml(raw).unwrap();
        let task = &config.tasks[0];
        assert_eq!(task.side, Some(OrderSide::Sell));
        assert_eq!(task.symbols.len(), 1);
        assert_eq!(task.symbols[0].instrument.ticker, "MSFT");
        assert_eq!(task.symbols[0].weight, Some(1.0));
    }

    #[test]
    fn rejects_unknown_task_template() {
        let raw = r#"[defaults]
timezone = "UTC"

[brokers.paper]
kind = "ibkr"

[[tasks]]
name = "buy-aapl"
template = "missing"
side = "buy"
pricing = { kind = "counterparty" }
shared_budget = { amount = 1000.0 }

[[tasks.symbols]]
ticker = "AAPL"
market = "us"
weight = 1.0
"#;

        let err = AppConfig::from_toml(raw).unwrap_err();
        assert!(err.to_string().contains("unknown template `missing`"));
    }

    #[test]
    fn accepts_watch_email_notification() {
        let mut config = sample_config(sample_task());
        config.watch = WatchConfig {
            reload_debounce_seconds: 600,
            notify: Some(WatchNotificationConfig {
                email: Some(WatchEmailNotificationConfig {
                    to: vec!["ops@example.com".into()],
                    on: vec![
                        WatchNotificationEvent::TaskListLoaded,
                        WatchNotificationEvent::TaskListChanged,
                    ],
                }),
            }),
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn watch_config_defaults_reload_debounce_to_ten_minutes() {
        let config = AppConfig::from_toml(
            r#"
[brokers.ibkr]
kind = "ibkr"

[[tasks]]
name = "test"
broker = "ibkr"
action = "cancel"
all_open = true
"#,
        )
        .unwrap();

        assert_eq!(config.watch.reload_debounce_seconds, 600);
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
