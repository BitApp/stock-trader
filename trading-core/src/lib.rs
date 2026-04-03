mod allocation;
mod broker;
mod config;
mod engine;
mod errors;
mod models;

pub use allocation::{estimate_reference_price, materialize_orders};
pub use broker::{Broker, BrokerFactory, BrokerRegistry};
pub use config::{
    AppConfig, BrokerConfig, ConfigFragment, DefaultsConfig, EmailNotificationConfig,
    EmailTransportConfig, NotificationEvent, ScheduleOverduePolicy, ScheduleWeekday, TaskConfig,
    TaskFieldsConfig, TaskNotificationConfig, TaskScheduleConfig, WatchConfig,
    WatchEmailNotificationConfig, WatchNotificationConfig, WatchNotificationEvent,
};
pub use engine::TradingEngine;
pub use errors::{Result, TradeBotError};
pub use models::{
    BrokerHealth, BrokerOrderRequest, BrokerOrderType, CancelOrderReport, CancelRequest,
    CancelResult, ExecutionPolicy, ExecutionResult, InstrumentRef, Market, OpenOrdersReport,
    OrderResult, OrderSide, OrderStatusReport, OrderStatusSnapshot, PositionSnapshot, PricingSpec,
    Quote, ResolvedInstrument, RiskPolicy, SessionPolicy, SharedBudget, SymbolTarget, TaskAction,
    TimeInForce, ValidationReport,
};
