use std::{collections::HashMap, sync::Arc};

use crate::{
    config::BrokerConfig,
    errors::{Result, TradeBotError},
    models::{
        BrokerHealth, BrokerOrderRequest, CancelRequest, CancelResult, OrderResult,
        OrderStatusSnapshot, PositionSnapshot, Quote, ResolvedInstrument,
    },
};

pub trait Broker: Send + Sync {
    fn broker_name(&self) -> &str;
    fn broker_kind(&self) -> &str;
    fn health_check(&self) -> Result<BrokerHealth>;
    fn resolve_instrument(
        &self,
        instrument: &crate::models::InstrumentRef,
    ) -> Result<ResolvedInstrument>;
    fn fetch_position(&self, instrument: &ResolvedInstrument) -> Result<PositionSnapshot>;
    fn fetch_quote(&self, instrument: &ResolvedInstrument) -> Result<Quote>;
    fn place_orders(&self, orders: &[BrokerOrderRequest]) -> Result<Vec<OrderResult>>;
    fn cancel_orders(&self, request: &CancelRequest) -> Result<Vec<CancelResult>>;
    fn cancel_order_by_id(&self, _broker_order_id: &str) -> Result<CancelResult> {
        Err(TradeBotError::Unsupported(format!(
            "broker `{}` does not support cancel_order_by_id",
            self.broker_kind()
        )))
    }
    fn get_order_status(&self, _broker_order_id: &str) -> Result<OrderStatusSnapshot> {
        Err(TradeBotError::Unsupported(format!(
            "broker `{}` does not support get_order_status",
            self.broker_kind()
        )))
    }
}

pub trait BrokerFactory: Send + Sync {
    fn kind(&self) -> &'static str;
    fn build(&self, broker_name: &str, config: &BrokerConfig) -> Result<Box<dyn Broker>>;
}

#[derive(Default, Clone)]
pub struct BrokerRegistry {
    factories: HashMap<String, Arc<dyn BrokerFactory>>,
}

impl BrokerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<F>(&mut self, factory: F)
    where
        F: BrokerFactory + 'static,
    {
        self.factories
            .insert(factory.kind().to_string(), Arc::new(factory));
    }

    pub fn build(&self, broker_name: &str, config: &BrokerConfig) -> Result<Box<dyn Broker>> {
        let factory = self.factories.get(&config.kind).ok_or_else(|| {
            TradeBotError::Config(format!(
                "no factory registered for broker kind `{}`",
                config.kind
            ))
        })?;
        factory.build(broker_name, config)
    }
}
