mod allocation;
mod broker;
mod config;
mod engine;
mod errors;
mod models;

pub use allocation::{estimate_reference_price, materialize_orders};
pub use broker::{Broker, BrokerFactory, BrokerRegistry};
pub use config::{AppConfig, BrokerConfig, DefaultsConfig, TaskConfig};
pub use engine::TradingEngine;
pub use errors::{Result, TradeBotError};
pub use models::{
    BrokerHealth, BrokerOrderRequest, BrokerOrderType, CancelRequest, CancelResult,
    ExecutionResult, InstrumentRef, Market, OrderResult, OrderSide, PricingSpec, Quote,
    ResolvedInstrument, RiskPolicy, SharedBudget, SymbolTarget, TaskAction, TimeInForce,
    ValidationReport,
};
