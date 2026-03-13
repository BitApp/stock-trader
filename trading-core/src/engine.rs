use crate::{
    allocation::materialize_orders,
    broker::BrokerRegistry,
    config::AppConfig,
    errors::Result,
    models::{CancelRequest, ExecutionResult, TaskAction, ValidationReport},
};

#[derive(Clone)]
pub struct TradingEngine {
    config: AppConfig,
    registry: BrokerRegistry,
}

impl TradingEngine {
    pub fn new(config: AppConfig, registry: BrokerRegistry) -> Self {
        Self { config, registry }
    }

    pub fn validate_task(&self, task_name: &str) -> Result<ValidationReport> {
        let task = self.config.task(task_name)?;
        let broker_config = self.config.broker(&task.broker)?;
        let broker = self.registry.build(&task.broker, broker_config)?;
        let health = broker.health_check()?;

        Ok(ValidationReport {
            task_name: task.name.clone(),
            broker_name: task.broker.clone(),
            broker_kind: broker_config.kind.clone(),
            health,
        })
    }

    pub fn run_task(&self, task_name: &str) -> Result<ExecutionResult> {
        let task = self.config.task(task_name)?;
        let broker_config = self.config.broker(&task.broker)?;
        let broker = self.registry.build(&task.broker, broker_config)?;

        let mut warnings = Vec::new();
        let health = broker.health_check()?;
        if !health.reachable || !health.authenticated {
            if let Some(message) = health.message {
                warnings.push(message);
            }
        }

        match task.action {
            TaskAction::Place => {
                let orders = materialize_orders(
                    task,
                    &self.config.defaults,
                    |instrument| broker.resolve_instrument(instrument),
                    |resolved| broker.fetch_quote(resolved),
                )?;
                let placed = broker.place_orders(&orders)?;
                Ok(ExecutionResult {
                    task_name: task.name.clone(),
                    broker_name: task.broker.clone(),
                    broker_kind: broker_config.kind.clone(),
                    action: TaskAction::Place,
                    orders: placed,
                    cancellations: Vec::new(),
                    warnings,
                })
            }
            TaskAction::Cancel => {
                let instruments = task
                    .symbols
                    .iter()
                    .map(|symbol| broker.resolve_instrument(&symbol.instrument))
                    .collect::<Result<Vec<_>>>()?;
                let request = CancelRequest {
                    broker_name: task.broker.clone(),
                    broker_kind: broker_config.kind.clone(),
                    instruments,
                    side: task.side,
                    client_tag: task.client_tag.clone(),
                    all_open: task.all_open,
                };
                let cancelled = broker.cancel_orders(&request)?;
                Ok(ExecutionResult {
                    task_name: task.name.clone(),
                    broker_name: task.broker.clone(),
                    broker_kind: broker_config.kind.clone(),
                    action: TaskAction::Cancel,
                    orders: Vec::new(),
                    cancellations: cancelled,
                    warnings,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use crate::{
        broker::{Broker, BrokerFactory, BrokerRegistry},
        config::{AppConfig, BrokerConfig, DefaultsConfig, TaskConfig},
        errors::Result,
        models::{
            BrokerHealth, BrokerOrderRequest, BrokerOrderType, CancelRequest, CancelResult,
            InstrumentRef, Market, OrderResult, OrderSide, PricingSpec, Quote, ResolvedInstrument,
            RiskPolicy, SharedBudget, SymbolTarget, TaskAction, TimeInForce,
        },
    };

    use super::TradingEngine;

    struct MockFactory;

    impl BrokerFactory for MockFactory {
        fn kind(&self) -> &'static str {
            "mock"
        }

        fn build(&self, broker_name: &str, _config: &BrokerConfig) -> Result<Box<dyn Broker>> {
            Ok(Box::new(MockBroker {
                name: broker_name.to_string(),
            }))
        }
    }

    struct MockBroker {
        name: String,
    }

    impl Broker for MockBroker {
        fn broker_name(&self) -> &str {
            &self.name
        }

        fn broker_kind(&self) -> &str {
            "mock"
        }

        fn health_check(&self) -> Result<BrokerHealth> {
            Ok(BrokerHealth {
                reachable: true,
                authenticated: true,
                brokerage_session: true,
                message: None,
            })
        }

        fn resolve_instrument(&self, instrument: &InstrumentRef) -> Result<ResolvedInstrument> {
            Ok(ResolvedInstrument {
                ticker: instrument.ticker.clone(),
                market: instrument.market,
                broker_symbol: format!(
                    "{}.{}",
                    instrument.ticker,
                    instrument.market.longbridge_suffix()
                ),
                conid: instrument.conid.clone(),
            })
        }

        fn fetch_quote(&self, _instrument: &ResolvedInstrument) -> Result<Quote> {
            Ok(Quote {
                bid: Some(99.0),
                ask: Some(100.0),
                last: Some(99.5),
                currency: Some("USD".into()),
            })
        }

        fn place_orders(&self, orders: &[BrokerOrderRequest]) -> Result<Vec<OrderResult>> {
            Ok(orders
                .iter()
                .map(|order| OrderResult {
                    broker_order_id: format!("order-{}", order.client_order_id),
                    client_order_id: order.client_order_id.clone(),
                    status: match order.order_type {
                        BrokerOrderType::Market => "submitted_market".into(),
                        BrokerOrderType::Limit => "submitted_limit".into(),
                    },
                    filled_qty: None,
                    avg_price: order.limit_price,
                    message: None,
                    raw_metadata: json!({ "symbol": order.instrument.broker_symbol }),
                })
                .collect())
        }

        fn cancel_orders(&self, request: &CancelRequest) -> Result<Vec<CancelResult>> {
            Ok(request
                .instruments
                .iter()
                .map(|instrument| CancelResult {
                    broker_order_id: format!("cancel-{}", instrument.broker_symbol),
                    status: "cancel_submitted".into(),
                    message: None,
                    raw_metadata: json!({ "symbol": instrument.broker_symbol }),
                })
                .collect())
        }
    }

    fn test_engine(task: TaskConfig) -> TradingEngine {
        let config = AppConfig {
            defaults: DefaultsConfig::default(),
            brokers: BTreeMap::from([(
                "paper".into(),
                BrokerConfig {
                    kind: "mock".into(),
                    settings: BTreeMap::new(),
                },
            )]),
            tasks: vec![task],
        };
        let mut registry = BrokerRegistry::new();
        registry.register(MockFactory);
        TradingEngine::new(config, registry)
    }

    #[test]
    fn run_place_task_materializes_weighted_orders() {
        let task = TaskConfig {
            name: "rebalance".into(),
            broker: "paper".into(),
            action: TaskAction::Place,
            side: Some(OrderSide::Buy),
            pricing: Some(PricingSpec::Counterparty),
            risk: Some(RiskPolicy { allow_margin: true }),
            shared_budget: Some(SharedBudget { amount: 1000.0 }),
            time_in_force: Some(TimeInForce::Day),
            client_tag: Some("run-1".into()),
            all_open: false,
            symbols: vec![
                SymbolTarget {
                    instrument: InstrumentRef {
                        ticker: "AAPL".into(),
                        market: Market::Us,
                        broker_symbol: None,
                        conid: Some("1".into()),
                    },
                    quantity: None,
                    amount: None,
                    weight: Some(0.6),
                    limit_price: None,
                    client_order_id: None,
                    broker_options: BTreeMap::new(),
                },
                SymbolTarget {
                    instrument: InstrumentRef {
                        ticker: "MSFT".into(),
                        market: Market::Us,
                        broker_symbol: None,
                        conid: Some("2".into()),
                    },
                    quantity: None,
                    amount: None,
                    weight: Some(0.4),
                    limit_price: None,
                    client_order_id: None,
                    broker_options: BTreeMap::new(),
                },
            ],
        };

        let result = test_engine(task).run_task("rebalance").unwrap();
        assert_eq!(result.orders.len(), 2);
        assert!(
            result
                .orders
                .iter()
                .all(|order| order.status == "submitted_limit")
        );
    }

    #[test]
    fn run_cancel_task_returns_cancel_results() {
        let task = TaskConfig {
            name: "cancel".into(),
            broker: "paper".into(),
            action: TaskAction::Cancel,
            side: Some(OrderSide::Sell),
            pricing: None,
            risk: None,
            shared_budget: None,
            time_in_force: None,
            client_tag: Some("run-2".into()),
            all_open: false,
            symbols: vec![SymbolTarget {
                instrument: InstrumentRef {
                    ticker: "700".into(),
                    market: Market::Hk,
                    broker_symbol: None,
                    conid: None,
                },
                quantity: None,
                amount: None,
                weight: None,
                limit_price: None,
                client_order_id: None,
                broker_options: BTreeMap::new(),
            }],
        };

        let result = test_engine(task).run_task("cancel").unwrap();
        assert_eq!(result.cancellations.len(), 1);
        assert_eq!(result.cancellations[0].status, "cancel_submitted");
    }
}
