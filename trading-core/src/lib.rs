mod allocation;
mod broker;
mod config;
mod engine;
mod errors;
mod models;

pub use allocation::{estimate_reference_price, materialize_orders};
pub use broker::{Broker, BrokerFactory, BrokerRegistry};
pub use config::{
    AppConfig, BrokerConfig, DefaultsConfig, EmailNotificationConfig, EmailTransportConfig,
    NotificationEvent, ScheduleWeekday, TaskConfig, TaskNotificationConfig, TaskScheduleConfig,
};
pub use engine::TradingEngine;
pub use errors::{Result, TradeBotError};
pub use models::{
    BrokerHealth, BrokerOrderRequest, BrokerOrderType, CancelRequest, CancelResult,
    ExecutionPolicy, ExecutionResult, InstrumentRef, Market, OrderResult, OrderSide,
    OrderStatusSnapshot, PositionSnapshot, PricingSpec, Quote, ResolvedInstrument, RiskPolicy,
    SessionPolicy, SharedBudget, SymbolTarget, TaskAction, TimeInForce, ValidationReport,
};
