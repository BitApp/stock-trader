use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskAction {
    Place,
    Cancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    pub fn as_uppercase(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Market {
    Us,
    Hk,
}

impl Market {
    pub fn longbridge_suffix(self) -> &'static str {
        match self {
            Self::Us => "US",
            Self::Hk => "HK",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    Day,
    Gtc,
}

impl TimeInForce {
    pub fn as_ibkr(self) -> &'static str {
        match self {
            Self::Day => "DAY",
            Self::Gtc => "GTC",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PricingSpec {
    Market,
    Limit { price: Option<f64> },
    Counterparty,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExecutionPolicy {
    OneShot,
    Track {
        timeout_seconds: u64,
        #[serde(default = "default_poll_seconds")]
        poll_seconds: u64,
    },
    CancelReplace {
        timeout_seconds: u64,
        #[serde(default = "default_poll_seconds")]
        poll_seconds: u64,
        #[serde(default = "default_max_attempts")]
        max_attempts: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SharedBudget {
    pub amount: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskPolicy {
    #[serde(default)]
    pub allow_margin: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionPolicy {
    #[serde(default)]
    pub extended_hours: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstrumentRef {
    pub ticker: String,
    pub market: Market,
    #[serde(default)]
    pub broker_symbol: Option<String>,
    #[serde(default)]
    pub conid: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolTarget {
    #[serde(flatten)]
    pub instrument: InstrumentRef,
    #[serde(default)]
    pub close_position: bool,
    #[serde(default)]
    pub quantity: Option<u64>,
    #[serde(default)]
    pub amount: Option<f64>,
    #[serde(default)]
    pub weight: Option<f64>,
    #[serde(default)]
    pub min_quantity: Option<u64>,
    #[serde(default)]
    pub quantity_step: Option<u64>,
    #[serde(default)]
    pub limit_price: Option<f64>,
    #[serde(default)]
    pub client_order_id: Option<String>,
    #[serde(default)]
    pub broker_options: BTreeMap<String, toml::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedInstrument {
    pub ticker: String,
    pub market: Market,
    pub broker_symbol: String,
    #[serde(default)]
    pub conid: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Quote {
    #[serde(default)]
    pub bid: Option<f64>,
    #[serde(default)]
    pub ask: Option<f64>,
    #[serde(default)]
    pub last: Option<f64>,
    #[serde(default)]
    pub currency: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub instrument: ResolvedInstrument,
    pub quantity: u64,
    pub available_quantity: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrokerOrderType {
    Market,
    Limit,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BrokerOrderRequest {
    pub instrument: ResolvedInstrument,
    pub side: OrderSide,
    pub order_type: BrokerOrderType,
    pub quantity: u64,
    #[serde(default)]
    pub limit_price: Option<f64>,
    pub tif: TimeInForce,
    pub allow_margin: bool,
    #[serde(default)]
    pub extended_hours: bool,
    pub client_order_id: String,
    #[serde(default)]
    pub client_tag: Option<String>,
    #[serde(default)]
    pub raw_metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelRequest {
    pub broker_name: String,
    pub broker_kind: String,
    pub instruments: Vec<ResolvedInstrument>,
    #[serde(default)]
    pub side: Option<OrderSide>,
    #[serde(default)]
    pub client_tag: Option<String>,
    #[serde(default)]
    pub all_open: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderResult {
    pub broker_order_id: String,
    pub client_order_id: String,
    pub status: String,
    #[serde(default)]
    pub filled_qty: Option<f64>,
    #[serde(default)]
    pub avg_price: Option<f64>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub raw_metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelResult {
    pub broker_order_id: String,
    pub status: String,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub raw_metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderStatusSnapshot {
    pub broker_order_id: String,
    pub status: String,
    #[serde(default)]
    pub filled_qty: Option<f64>,
    #[serde(default)]
    pub remaining_qty: Option<f64>,
    #[serde(default)]
    pub avg_price: Option<f64>,
    #[serde(default)]
    pub message: Option<String>,
    pub is_active: bool,
    pub is_final: bool,
    #[serde(default)]
    pub raw_metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub task_name: String,
    pub broker_name: String,
    pub broker_kind: String,
    pub action: TaskAction,
    #[serde(default)]
    pub orders: Vec<OrderResult>,
    #[serde(default)]
    pub cancellations: Vec<CancelResult>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BrokerHealth {
    pub reachable: bool,
    pub authenticated: bool,
    pub brokerage_session: bool,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidationReport {
    pub task_name: String,
    pub broker_name: String,
    pub broker_kind: String,
    pub health: BrokerHealth,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderStatusReport {
    pub broker_name: String,
    pub broker_kind: String,
    pub order: OrderStatusSnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenOrdersReport {
    pub broker_name: String,
    pub broker_kind: String,
    #[serde(default)]
    pub orders: Vec<OrderStatusSnapshot>,
}

fn default_poll_seconds() -> u64 {
    5
}

fn default_max_attempts() -> u32 {
    1
}
