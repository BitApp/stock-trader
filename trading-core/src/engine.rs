use std::{
    thread,
    time::{Duration, Instant},
};

use crate::{
    allocation::materialize_orders,
    broker::BrokerRegistry,
    config::AppConfig,
    errors::Result,
    models::{
        CancelRequest, CancelResult, ExecutionPolicy, ExecutionResult, OrderResult,
        OrderStatusSnapshot, SymbolTarget, TaskAction, TaskAction::Place, ValidationReport,
    },
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
                if matches!(
                    task.execution,
                    Some(ExecutionPolicy::Track { .. } | ExecutionPolicy::CancelReplace { .. })
                ) {
                    let execution = task
                        .execution
                        .as_ref()
                        .expect("matched Some execution policy");
                    self.run_managed_place_task(
                        task,
                        broker.as_ref(),
                        broker_config.kind.clone(),
                        warnings,
                        execution,
                    )
                } else {
                    let orders = materialize_orders(
                        task,
                        &self.config.defaults,
                        |instrument| broker.resolve_instrument(instrument),
                        |resolved| broker.fetch_position(resolved),
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

    fn run_managed_place_task(
        &self,
        task: &crate::config::TaskConfig,
        broker: &dyn crate::Broker,
        broker_kind: String,
        mut warnings: Vec<String>,
        execution: &ExecutionPolicy,
    ) -> Result<ExecutionResult> {
        let seed_orders = materialize_orders(
            task,
            &self.config.defaults,
            |instrument| broker.resolve_instrument(instrument),
            |resolved| broker.fetch_position(resolved),
            |resolved| broker.fetch_quote(resolved),
        )?;

        let mut placed_orders = Vec::new();
        let mut cancellations = Vec::new();

        for (symbol, seed_order) in task.symbols.iter().zip(seed_orders.iter()) {
            let (mut symbol_orders, mut symbol_cancellations) =
                self.execute_symbol_policy(task, symbol, seed_order, broker, execution)?;
            placed_orders.append(&mut symbol_orders);
            cancellations.append(&mut symbol_cancellations);
        }

        if placed_orders.iter().any(|order| {
            matches!(
                execution,
                ExecutionPolicy::Track { .. } | ExecutionPolicy::CancelReplace { .. }
                    if order.status == "tracking_timeout"
                        || order.status == "timeout_unfilled"
                        || order.status == "cancelled_for_retry"
            )
        }) {
            warnings.push(format!(
                "task `{}` left at least one order unfilled after managed execution",
                task.name
            ));
        }

        Ok(ExecutionResult {
            task_name: task.name.clone(),
            broker_name: task.broker.clone(),
            broker_kind,
            action: Place,
            orders: placed_orders,
            cancellations,
            warnings,
        })
    }

    fn execute_symbol_policy(
        &self,
        task: &crate::config::TaskConfig,
        symbol: &SymbolTarget,
        seed_order: &crate::BrokerOrderRequest,
        broker: &dyn crate::Broker,
        execution: &ExecutionPolicy,
    ) -> Result<(Vec<OrderResult>, Vec<CancelResult>)> {
        match execution {
            ExecutionPolicy::OneShot => unreachable!("one_shot does not use managed execution"),
            ExecutionPolicy::Track {
                timeout_seconds,
                poll_seconds,
            } => self.track_single_order(seed_order, broker, *timeout_seconds, *poll_seconds),
            ExecutionPolicy::CancelReplace {
                timeout_seconds,
                poll_seconds,
                max_attempts,
            } => self.execute_cancel_replace_policy(
                task,
                symbol,
                seed_order,
                broker,
                *timeout_seconds,
                *poll_seconds,
                *max_attempts,
            ),
        }
    }

    fn execute_cancel_replace_policy(
        &self,
        task: &crate::config::TaskConfig,
        symbol: &SymbolTarget,
        seed_order: &crate::BrokerOrderRequest,
        broker: &dyn crate::Broker,
        timeout_seconds: u64,
        poll_seconds: u64,
        max_attempts: u32,
    ) -> Result<(Vec<OrderResult>, Vec<CancelResult>)> {
        let mut remaining_quantity = seed_order.quantity;
        let mut orders = Vec::new();
        let mut cancellations = Vec::new();

        for attempt in 1..=max_attempts {
            let request = self.retry_order_request(task, symbol, remaining_quantity, broker)?;
            let mut placed = broker
                .place_orders(std::slice::from_ref(&request))?
                .into_iter()
                .next()
                .expect("broker returned empty order list for single order request");

            let snapshot = self.wait_for_order(
                broker,
                &placed.broker_order_id,
                timeout_seconds,
                poll_seconds,
            )?;
            apply_status_snapshot(&mut placed, &snapshot);

            let remaining_after_snapshot = snapshot
                .remaining_qty
                .map(normalize_qty)
                .transpose()?
                .unwrap_or_else(|| {
                    remaining_quantity.saturating_sub(snapshot_filled_qty(&snapshot))
                });

            let should_stop = snapshot.is_final || remaining_after_snapshot == 0;
            orders.push(placed);

            if should_stop {
                break;
            }

            if attempt == max_attempts {
                if let Some(last) = orders.last_mut() {
                    last.status = "timeout_unfilled".into();
                }
                break;
            }

            let cancellation = broker
                .cancel_order_by_id(&orders.last().expect("just pushed").broker_order_id.clone())?;
            cancellations.push(cancellation);
            if let Some(last) = orders.last_mut() {
                last.status = "cancelled_for_retry".into();
            }
            remaining_quantity = remaining_after_snapshot;
        }

        Ok((orders, cancellations))
    }

    fn track_single_order(
        &self,
        seed_order: &crate::BrokerOrderRequest,
        broker: &dyn crate::Broker,
        timeout_seconds: u64,
        poll_seconds: u64,
    ) -> Result<(Vec<OrderResult>, Vec<CancelResult>)> {
        let mut placed = broker
            .place_orders(std::slice::from_ref(seed_order))?
            .into_iter()
            .next()
            .expect("broker returned empty order list for single order request");

        let snapshot = self.wait_for_order(
            broker,
            &placed.broker_order_id,
            timeout_seconds,
            poll_seconds,
        )?;
        apply_status_snapshot(&mut placed, &snapshot);

        if !snapshot.is_final && snapshot.is_active {
            placed.status = "tracking_timeout".into();
        }

        Ok((vec![placed], Vec::new()))
    }

    fn retry_order_request(
        &self,
        task: &crate::config::TaskConfig,
        symbol: &SymbolTarget,
        remaining_quantity: u64,
        broker: &dyn crate::Broker,
    ) -> Result<crate::BrokerOrderRequest> {
        let mut task = task.clone();
        let mut symbol = symbol.clone();
        symbol.close_position = false;
        symbol.quantity = Some(remaining_quantity);
        symbol.amount = None;
        symbol.weight = None;
        task.shared_budget = None;
        task.symbols = vec![symbol];

        materialize_orders(
            &task,
            &self.config.defaults,
            |instrument| broker.resolve_instrument(instrument),
            |resolved| broker.fetch_position(resolved),
            |resolved| broker.fetch_quote(resolved),
        )
        .map(|mut orders| orders.remove(0))
    }

    fn wait_for_order(
        &self,
        broker: &dyn crate::Broker,
        broker_order_id: &str,
        timeout_seconds: u64,
        poll_seconds: u64,
    ) -> Result<OrderStatusSnapshot> {
        let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
        loop {
            let snapshot = broker.get_order_status(broker_order_id)?;
            if snapshot.is_final || !snapshot.is_active || timeout_seconds == 0 {
                return Ok(snapshot);
            }
            if Instant::now() >= deadline {
                return Ok(snapshot);
            }
            thread::sleep(Duration::from_secs(poll_seconds.max(1)));
        }
    }
}

fn apply_status_snapshot(order: &mut OrderResult, snapshot: &OrderStatusSnapshot) {
    order.status = snapshot.status.clone();
    order.filled_qty = snapshot.filled_qty;
    order.avg_price = snapshot.avg_price;
    order.message = snapshot.message.clone();
    order.raw_metadata = snapshot.raw_metadata.clone();
}

fn normalize_qty(quantity: f64) -> Result<u64> {
    if quantity < 0.0 {
        return Err(crate::TradeBotError::Validation(format!(
            "remaining quantity must be non-negative, got {quantity}"
        )));
    }
    Ok(quantity.floor() as u64)
}

fn snapshot_filled_qty(snapshot: &OrderStatusSnapshot) -> u64 {
    snapshot
        .filled_qty
        .map(|quantity| quantity.floor() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use serde_json::json;

    use crate::{
        broker::{Broker, BrokerFactory, BrokerRegistry},
        config::{AppConfig, BrokerConfig, DefaultsConfig, TaskConfig},
        errors::Result,
        models::{
            BrokerHealth, BrokerOrderRequest, BrokerOrderType, CancelRequest, CancelResult,
            ExecutionPolicy, InstrumentRef, Market, OrderResult, OrderSide, OrderStatusSnapshot,
            PositionSnapshot, PricingSpec, Quote, ResolvedInstrument, RiskPolicy, SharedBudget,
            SymbolTarget, TaskAction, TimeInForce,
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

        fn fetch_position(&self, instrument: &ResolvedInstrument) -> Result<PositionSnapshot> {
            Ok(PositionSnapshot {
                instrument: instrument.clone(),
                quantity: 100,
                available_quantity: 100,
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
        test_engine_with_registry(task, {
            let mut registry = BrokerRegistry::new();
            registry.register(MockFactory);
            registry
        })
    }

    fn test_engine_with_registry(task: TaskConfig, registry: BrokerRegistry) -> TradingEngine {
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
        TradingEngine::new(config, registry)
    }

    #[derive(Default)]
    struct RetryState {
        placed_quantities: Vec<u64>,
        cancelled_order_ids: Vec<String>,
        next_order_id: usize,
    }

    struct RetryFactory {
        state: Arc<Mutex<RetryState>>,
    }

    impl BrokerFactory for RetryFactory {
        fn kind(&self) -> &'static str {
            "retry_mock"
        }

        fn build(&self, broker_name: &str, _config: &BrokerConfig) -> Result<Box<dyn Broker>> {
            Ok(Box::new(RetryBroker {
                name: broker_name.to_string(),
                state: self.state.clone(),
            }))
        }
    }

    struct RetryBroker {
        name: String,
        state: Arc<Mutex<RetryState>>,
    }

    impl Broker for RetryBroker {
        fn broker_name(&self) -> &str {
            &self.name
        }

        fn broker_kind(&self) -> &str {
            "retry_mock"
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
                broker_symbol: format!("{}.US", instrument.ticker),
                conid: None,
            })
        }

        fn fetch_position(&self, instrument: &ResolvedInstrument) -> Result<PositionSnapshot> {
            Ok(PositionSnapshot {
                instrument: instrument.clone(),
                quantity: 100,
                available_quantity: 100,
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
            let mut state = self.state.lock().unwrap();
            orders
                .iter()
                .map(|order| {
                    state.next_order_id += 1;
                    state.placed_quantities.push(order.quantity);
                    Ok(OrderResult {
                        broker_order_id: format!("order-{}", state.next_order_id),
                        client_order_id: order.client_order_id.clone(),
                        status: "submitted_limit".into(),
                        filled_qty: None,
                        avg_price: order.limit_price,
                        message: None,
                        raw_metadata: json!({}),
                    })
                })
                .collect()
        }

        fn cancel_orders(&self, _request: &CancelRequest) -> Result<Vec<CancelResult>> {
            Ok(Vec::new())
        }

        fn cancel_order_by_id(&self, broker_order_id: &str) -> Result<CancelResult> {
            self.state
                .lock()
                .unwrap()
                .cancelled_order_ids
                .push(broker_order_id.to_string());
            Ok(CancelResult {
                broker_order_id: broker_order_id.to_string(),
                status: "cancel_submitted".into(),
                message: None,
                raw_metadata: json!({}),
            })
        }

        fn get_order_status(&self, broker_order_id: &str) -> Result<OrderStatusSnapshot> {
            match broker_order_id {
                "order-1" => Ok(OrderStatusSnapshot {
                    broker_order_id: broker_order_id.to_string(),
                    status: "new".into(),
                    filled_qty: Some(0.0),
                    remaining_qty: Some(10.0),
                    avg_price: Some(99.0),
                    message: None,
                    is_active: true,
                    is_final: false,
                    raw_metadata: json!({}),
                }),
                _ => Ok(OrderStatusSnapshot {
                    broker_order_id: broker_order_id.to_string(),
                    status: "filled".into(),
                    filled_qty: Some(10.0),
                    remaining_qty: Some(0.0),
                    avg_price: Some(99.0),
                    message: None,
                    is_active: false,
                    is_final: true,
                    raw_metadata: json!({}),
                }),
            }
        }
    }

    #[test]
    fn run_place_task_materializes_weighted_orders() {
        let task = TaskConfig {
            name: "rebalance".into(),
            broker: "paper".into(),
            action: TaskAction::Place,
            note: None,
            schedule: None,
            execution: None,
            notify: None,
            side: Some(OrderSide::Buy),
            pricing: Some(PricingSpec::Counterparty),
            risk: Some(RiskPolicy { allow_margin: true }),
            session: None,
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
                    close_position: false,
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
                    close_position: false,
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
            note: None,
            schedule: None,
            execution: None,
            notify: None,
            side: Some(OrderSide::Sell),
            pricing: None,
            risk: None,
            session: None,
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
                close_position: false,
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

    #[test]
    fn run_place_task_retries_with_cancel_replace_policy() {
        let state = Arc::new(Mutex::new(RetryState::default()));
        let mut registry = BrokerRegistry::new();
        registry.register(RetryFactory {
            state: state.clone(),
        });

        let task = TaskConfig {
            name: "retry".into(),
            broker: "paper".into(),
            action: TaskAction::Place,
            note: None,
            schedule: None,
            execution: Some(ExecutionPolicy::CancelReplace {
                timeout_seconds: 0,
                poll_seconds: 1,
                max_attempts: 2,
            }),
            notify: None,
            side: Some(OrderSide::Buy),
            pricing: Some(PricingSpec::Counterparty),
            risk: None,
            session: None,
            shared_budget: None,
            time_in_force: Some(TimeInForce::Day),
            client_tag: Some("retry-run".into()),
            all_open: false,
            symbols: vec![SymbolTarget {
                instrument: InstrumentRef {
                    ticker: "AAPL".into(),
                    market: Market::Us,
                    broker_symbol: None,
                    conid: None,
                },
                close_position: false,
                quantity: Some(10),
                amount: None,
                weight: None,
                limit_price: None,
                client_order_id: None,
                broker_options: BTreeMap::new(),
            }],
        };

        let config = AppConfig {
            defaults: DefaultsConfig::default(),
            brokers: BTreeMap::from([(
                "paper".into(),
                BrokerConfig {
                    kind: "retry_mock".into(),
                    settings: BTreeMap::new(),
                },
            )]),
            tasks: vec![task],
        };

        let result = TradingEngine::new(config, registry)
            .run_task("retry")
            .unwrap();
        let state = state.lock().unwrap();

        assert_eq!(result.orders.len(), 2);
        assert_eq!(result.cancellations.len(), 1);
        assert_eq!(result.orders[0].status, "cancelled_for_retry");
        assert_eq!(result.orders[1].status, "filled");
        assert_eq!(state.placed_quantities, vec![10, 10]);
        assert_eq!(state.cancelled_order_ids, vec!["order-1".to_string()]);
    }

    struct TrackFactory;

    impl BrokerFactory for TrackFactory {
        fn kind(&self) -> &'static str {
            "track_mock"
        }

        fn build(&self, broker_name: &str, _config: &BrokerConfig) -> Result<Box<dyn Broker>> {
            Ok(Box::new(TrackBroker {
                name: broker_name.to_string(),
            }))
        }
    }

    struct TrackBroker {
        name: String,
    }

    impl Broker for TrackBroker {
        fn broker_name(&self) -> &str {
            &self.name
        }

        fn broker_kind(&self) -> &str {
            "track_mock"
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
                broker_symbol: format!("{}.US", instrument.ticker),
                conid: None,
            })
        }

        fn fetch_position(&self, instrument: &ResolvedInstrument) -> Result<PositionSnapshot> {
            Ok(PositionSnapshot {
                instrument: instrument.clone(),
                quantity: 100,
                available_quantity: 100,
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
                    broker_order_id: format!("track-{}", order.client_order_id),
                    client_order_id: order.client_order_id.clone(),
                    status: "submitted_market".into(),
                    filled_qty: None,
                    avg_price: None,
                    message: None,
                    raw_metadata: json!({ "symbol": order.instrument.broker_symbol }),
                })
                .collect())
        }

        fn cancel_orders(&self, _request: &CancelRequest) -> Result<Vec<CancelResult>> {
            Ok(Vec::new())
        }

        fn get_order_status(&self, broker_order_id: &str) -> Result<OrderStatusSnapshot> {
            Ok(OrderStatusSnapshot {
                broker_order_id: broker_order_id.to_string(),
                status: "filled".into(),
                filled_qty: Some(10.0),
                remaining_qty: Some(0.0),
                avg_price: Some(99.0),
                message: None,
                is_active: false,
                is_final: true,
                raw_metadata: json!({ "symbol": "AAPL.US" }),
            })
        }
    }

    #[test]
    fn run_place_task_tracks_without_retry() {
        let mut registry = BrokerRegistry::new();
        registry.register(TrackFactory);

        let task = TaskConfig {
            name: "track".into(),
            broker: "paper".into(),
            action: TaskAction::Place,
            note: None,
            schedule: None,
            execution: Some(ExecutionPolicy::Track {
                timeout_seconds: 30,
                poll_seconds: 1,
            }),
            notify: None,
            side: Some(OrderSide::Sell),
            pricing: Some(PricingSpec::Market),
            risk: None,
            session: None,
            shared_budget: None,
            time_in_force: Some(TimeInForce::Day),
            client_tag: Some("track-run".into()),
            all_open: false,
            symbols: vec![SymbolTarget {
                instrument: InstrumentRef {
                    ticker: "AAPL".into(),
                    market: Market::Us,
                    broker_symbol: None,
                    conid: None,
                },
                close_position: false,
                quantity: Some(10),
                amount: None,
                weight: None,
                limit_price: None,
                client_order_id: None,
                broker_options: BTreeMap::new(),
            }],
        };

        let config = AppConfig {
            defaults: DefaultsConfig::default(),
            brokers: BTreeMap::from([(
                "paper".into(),
                BrokerConfig {
                    kind: "track_mock".into(),
                    settings: BTreeMap::new(),
                },
            )]),
            tasks: vec![task],
        };

        let result = TradingEngine::new(config, registry)
            .run_task("track")
            .unwrap();

        assert_eq!(result.orders.len(), 1);
        assert_eq!(result.orders[0].status, "filled");
        assert!(result.cancellations.is_empty());
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn run_place_task_explicit_one_shot_matches_plain_submit() {
        let task = TaskConfig {
            name: "one-shot".into(),
            broker: "paper".into(),
            action: TaskAction::Place,
            note: None,
            schedule: None,
            execution: Some(ExecutionPolicy::OneShot),
            notify: None,
            side: Some(OrderSide::Buy),
            pricing: Some(PricingSpec::Counterparty),
            risk: None,
            session: None,
            shared_budget: None,
            time_in_force: Some(TimeInForce::Day),
            client_tag: Some("one-shot-run".into()),
            all_open: false,
            symbols: vec![SymbolTarget {
                instrument: InstrumentRef {
                    ticker: "AAPL".into(),
                    market: Market::Us,
                    broker_symbol: None,
                    conid: None,
                },
                close_position: false,
                quantity: Some(10),
                amount: None,
                weight: None,
                limit_price: None,
                client_order_id: None,
                broker_options: BTreeMap::new(),
            }],
        };

        let result = test_engine(task).run_task("one-shot").unwrap();

        assert_eq!(result.orders.len(), 1);
        assert_eq!(result.orders[0].status, "submitted_limit");
        assert!(result.cancellations.is_empty());
        assert!(result.warnings.is_empty());
    }
}
