use std::{collections::BTreeMap, fs, path::Path};

use chrono::{NaiveTime, Weekday};
use chrono_tz::Tz;
use serde::Deserialize;

use crate::{
    errors::{Result, TradeBotError},
    models::{
        OrderSide, PricingSpec, RiskPolicy, SharedBudget, SymbolTarget, TaskAction, TimeInForce,
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
}

impl Default for DefaultsConfig {
    fn default() -> Self {
        Self {
            timezone: default_timezone(),
            default_tif: default_tif(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BrokerConfig {
    pub kind: String,
    #[serde(flatten)]
    pub settings: BTreeMap<String, toml::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaskConfig {
    pub name: String,
    pub broker: String,
    pub action: TaskAction,
    #[serde(default)]
    pub schedule: Option<TaskScheduleConfig>,
    #[serde(default)]
    pub side: Option<OrderSide>,
    #[serde(default)]
    pub pricing: Option<PricingSpec>,
    #[serde(default)]
    pub risk: Option<RiskPolicy>,
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
pub struct TaskScheduleConfig {
    pub time: String,
    #[serde(default)]
    pub weekdays: Vec<ScheduleWeekday>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
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
        let mut weight_sum = 0.0;

        for symbol in &task.symbols {
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
            if (weight_sum - 0.0).abs() < f64::EPSILON {
                return Err(TradeBotError::Validation(format!(
                    "task `{}` shared_budget requires symbol weights",
                    task.name
                )));
            }
        } else if !has_explicit_allocation {
            return Err(TradeBotError::Validation(format!(
                "task `{}` requires quantity/amount or shared_budget",
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

fn parse_schedule_time(raw: &str) -> Option<NaiveTime> {
    const FORMATS: [&str; 2] = ["%H:%M", "%H:%M:%S"];

    FORMATS
        .iter()
        .find_map(|format| NaiveTime::parse_from_str(raw, format).ok())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{AppConfig, BrokerConfig, DefaultsConfig, TaskConfig, TaskScheduleConfig};
    use crate::{
        errors::TradeBotError,
        models::{
            InstrumentRef, Market, OrderSide, PricingSpec, SharedBudget, SymbolTarget, TaskAction,
        },
    };

    fn sample_task() -> TaskConfig {
        TaskConfig {
            name: "scheduled-rebalance".into(),
            broker: "paper".into(),
            action: TaskAction::Place,
            schedule: Some(TaskScheduleConfig {
                time: "09:30".into(),
                weekdays: Vec::new(),
                enabled: true,
            }),
            side: Some(OrderSide::Buy),
            pricing: Some(PricingSpec::Counterparty),
            risk: None,
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
                quantity: None,
                amount: None,
                weight: Some(1.0),
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
    fn rejects_invalid_timezone() {
        let mut config = sample_config(sample_task());
        config.defaults.timezone = "Mars/Olympus".into();

        let err = config.validate().unwrap_err();
        assert!(
            matches!(err, TradeBotError::Config(message) if message.contains("invalid defaults.timezone"))
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
}
