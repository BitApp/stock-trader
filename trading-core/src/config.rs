use std::{collections::BTreeMap, fs, path::Path};

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

impl AppConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let raw = fs::read_to_string(path)?;
        let parsed: Self = toml::from_str(&raw)?;
        parsed.validate()?;
        Ok(parsed)
    }

    pub fn validate(&self) -> Result<()> {
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

fn default_timezone() -> String {
    "UTC".to_string()
}

fn default_tif() -> TimeInForce {
    TimeInForce::Day
}
