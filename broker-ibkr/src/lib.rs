use std::{collections::HashMap, time::Duration};

use ibapi::{
    accounts::PositionUpdate,
    client::blocking::Client,
    contracts::Contract,
    market_data::MarketDataType,
    market_data::realtime::{TickType, TickTypes},
    orders::{
        CancelOrder, ExecutionData, ExecutionFilter, Executions, OrderData,
        OrderStatus as IbOrderStatus, Orders, PlaceOrder, TimeInForce as IbTimeInForce,
    },
    subscriptions::sync::Subscription,
};
use serde_json::json;
use tracing::debug;
use trading_core::{
    Broker, BrokerConfig, BrokerFactory, BrokerHealth, BrokerOrderRequest, BrokerOrderType,
    CancelRequest, CancelResult, InstrumentRef, Market, OrderResult, OrderSide,
    OrderStatusSnapshot, PositionSnapshot, Quote, ResolvedInstrument, Result, TimeInForce,
    TradeBotError,
};

const SUBSCRIPTION_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Default)]
pub struct IbkrBrokerFactory;

impl BrokerFactory for IbkrBrokerFactory {
    fn kind(&self) -> &'static str {
        "ibkr"
    }

    fn build(&self, broker_name: &str, config: &BrokerConfig) -> Result<Box<dyn Broker>> {
        let settings = parse_settings(config)?;
        let address = format!("{}:{}", settings.host, settings.port);

        let (client, init_error) = match Client::connect(address.as_str(), settings.client_id) {
            Ok(client) => (Some(client), None),
            Err(err) => (None, Some(err.to_string())),
        };

        Ok(Box::new(IbkrBroker {
            name: broker_name.to_string(),
            settings,
            client,
            init_error,
        }))
    }
}

struct IbkrSettings {
    host: String,
    port: u16,
    client_id: i32,
    account_id: String,
}

struct IbkrBroker {
    name: String,
    settings: IbkrSettings,
    client: Option<Client>,
    init_error: Option<String>,
}

impl Broker for IbkrBroker {
    fn broker_name(&self) -> &str {
        &self.name
    }

    fn broker_kind(&self) -> &str {
        "ibkr"
    }

    fn health_check(&self) -> Result<BrokerHealth> {
        let Some(client) = &self.client else {
            return Ok(BrokerHealth {
                reachable: false,
                authenticated: false,
                brokerage_session: false,
                message: self.init_error.clone(),
            });
        };

        match client.managed_accounts() {
            Ok(accounts) => {
                let brokerage_session = accounts
                    .iter()
                    .any(|account| account == &self.settings.account_id);
                let message = (!brokerage_session).then(|| {
                    format!(
                        "IBKR account `{}` is not exposed by the current Gateway session",
                        self.settings.account_id
                    )
                });

                Ok(BrokerHealth {
                    reachable: true,
                    authenticated: true,
                    brokerage_session,
                    message,
                })
            }
            Err(err) => Ok(BrokerHealth {
                reachable: false,
                authenticated: false,
                brokerage_session: false,
                message: Some(err.to_string()),
            }),
        }
    }

    fn resolve_instrument(&self, instrument: &InstrumentRef) -> Result<ResolvedInstrument> {
        let ticker = instrument.ticker.to_ascii_uppercase();
        let broker_symbol = instrument
            .broker_symbol
            .as_ref()
            .map(|symbol| symbol.to_ascii_uppercase())
            .unwrap_or_else(|| ticker.clone());

        if let Some(conid) = instrument.conid.clone() {
            return Ok(ResolvedInstrument {
                ticker,
                market: instrument.market,
                broker_symbol,
                conid: Some(conid),
            });
        }

        let contract = self.lookup_contract(&ticker, instrument.market)?;

        Ok(ResolvedInstrument {
            ticker,
            market: instrument.market,
            broker_symbol: contract_display_symbol(&contract),
            conid: Some(contract.contract_id.to_string()),
        })
    }

    fn fetch_position(&self, instrument: &ResolvedInstrument) -> Result<PositionSnapshot> {
        let client = self.client()?;
        let positions = client.positions().map_err(map_ibkr_error)?;

        while let Some(update) = positions.next_timeout(SUBSCRIPTION_TIMEOUT) {
            match update {
                PositionUpdate::Position(position) => {
                    if position.account != self.settings.account_id {
                        continue;
                    }
                    if !contract_matches(&position.contract, instrument) {
                        continue;
                    }

                    let quantity = normalize_position_qty(position.position)?;
                    if quantity == 0 {
                        return Err(TradeBotError::Validation(format!(
                            "IBKR position not found for `{}`",
                            instrument.broker_symbol
                        )));
                    }

                    return Ok(PositionSnapshot {
                        instrument: instrument.clone(),
                        quantity,
                        available_quantity: quantity,
                    });
                }
                PositionUpdate::PositionEnd => break,
            }
        }
        if let Some(err) = positions.error() {
            return Err(map_ibkr_error(err));
        }

        Err(TradeBotError::Validation(format!(
            "IBKR position not found for `{}`",
            instrument.broker_symbol
        )))
    }

    fn fetch_quote(&self, instrument: &ResolvedInstrument) -> Result<Quote> {
        let client = self.client()?;
        let contract = contract_from_resolved(instrument)?;
        let realtime = snapshot_quote(client, &contract, instrument.market)?;
        if has_quote_value(&realtime) {
            return Ok(realtime);
        }

        client
            .switch_market_data_type(MarketDataType::Delayed)
            .map_err(map_ibkr_error)?;
        let delayed = snapshot_quote(client, &contract, instrument.market)?;
        if has_quote_value(&delayed) {
            return Ok(delayed);
        }

        Err(TradeBotError::broker(
            "ibkr",
            format!("missing quote for `{}`", instrument.broker_symbol),
        ))
    }

    fn place_orders(&self, orders: &[BrokerOrderRequest]) -> Result<Vec<OrderResult>> {
        let client = self.client()?;

        orders
            .iter()
            .map(|order| {
                let contract = contract_from_resolved(&order.instrument)?;
                let ib_order = build_ib_order(client, &self.settings, order)?;
                let order_id = client.next_order_id();
                let mut updates = client
                    .place_order(order_id, &contract, &ib_order)
                    .map_err(map_ibkr_error)?;
                let snapshot = collect_place_order_snapshot(order_id, &mut updates)?;

                Ok(order_result_from_place_snapshot(order_id, order, &snapshot))
            })
            .collect()
    }

    fn cancel_orders(&self, request: &CancelRequest) -> Result<Vec<CancelResult>> {
        let mut live_orders = self.read_orders(|client| client.all_open_orders())?;
        let mut results = Vec::new();

        for (order_id, row) in live_orders.drain() {
            if !matches_cancel_filter(&row, request) {
                continue;
            }
            results.push(self.cancel_order_by_id(&order_id.to_string())?);
        }

        Ok(results)
    }

    fn cancel_order_by_id(&self, broker_order_id: &str) -> Result<CancelResult> {
        let client = self.client()?;
        let order_id = parse_order_id(broker_order_id)?;
        let updates = client.cancel_order(order_id, "").map_err(map_ibkr_error)?;

        let mut status = None;
        let mut message = None;
        while let Some(update) = updates.next_timeout(SUBSCRIPTION_TIMEOUT) {
            match update {
                CancelOrder::OrderStatus(order_status) => {
                    status = Some(order_status.status.clone());
                }
                CancelOrder::Notice(notice) => {
                    message = Some(notice.message.clone());
                }
            }
        }
        if let Some(err) = updates.error() {
            return Err(map_ibkr_error(err));
        }

        Ok(CancelResult {
            broker_order_id: broker_order_id.to_string(),
            status: status.unwrap_or_else(|| "cancel_submitted".into()),
            message,
            raw_metadata: json!({ "order_id": order_id }),
        })
    }

    fn get_order_status(&self, broker_order_id: &str) -> Result<OrderStatusSnapshot> {
        let order_id = parse_order_id(broker_order_id)?;

        if let Some(snapshot) = self.query_live_order_status(order_id)? {
            return Ok(snapshot);
        }
        if let Some(snapshot) = self.query_completed_order_status(order_id)? {
            return Ok(snapshot);
        }
        if let Some(snapshot) = self.query_execution_status(order_id)? {
            return Ok(snapshot);
        }

        Err(TradeBotError::broker(
            "ibkr",
            format!("order `{broker_order_id}` not found"),
        ))
    }

    fn list_open_orders(&self) -> Result<Vec<OrderStatusSnapshot>> {
        let mut orders = self.read_live_orders()?;
        let mut snapshots = orders
            .drain()
            .filter_map(|(order_id, row)| {
                let snapshot = order_status_snapshot_from_order_row(order_id, row);
                snapshot.is_active.then_some(snapshot)
            })
            .collect::<Vec<_>>();
        snapshots.sort_by_key(|snapshot| snapshot.broker_order_id.parse::<i32>().unwrap_or(i32::MAX));
        Ok(snapshots)
    }
}

impl IbkrBroker {
    fn client(&self) -> Result<&Client> {
        self.client.as_ref().ok_or_else(|| {
            TradeBotError::broker(
                "ibkr",
                self.init_error
                    .clone()
                    .unwrap_or_else(|| "IBKR client is not connected".into()),
            )
        })
    }

    fn lookup_contract(&self, symbol: &str, market: Market) -> Result<Contract> {
        let client = self.client()?;
        let template = contract_template(symbol, market);
        let details = client.contract_details(&template).map_err(map_ibkr_error)?;

        for detail in details {
            if detail.contract.contract_id > 0 {
                return Ok(detail.contract);
            }
        }

        Err(TradeBotError::Validation(format!(
            "IBKR contract lookup failed for `{symbol}`"
        )))
    }

    fn read_orders(
        &self,
        fetch: impl FnOnce(&Client) -> std::result::Result<Subscription<Orders>, ibapi::Error>,
    ) -> Result<HashMap<i32, OrderSnapshotData>> {
        let client = self.client()?;
        let stream = fetch(client).map_err(map_ibkr_error)?;
        let mut rows: HashMap<i32, OrderSnapshotData> = HashMap::new();
        let mut last_notice = None;

        while let Some(update) = stream.next_timeout(SUBSCRIPTION_TIMEOUT) {
            match update {
                Orders::OrderData(data) => {
                    let order_id = data.order_id;
                    rows.entry(order_id).or_default().data = Some(data);
                }
                Orders::OrderStatus(status) => {
                    let order_id = status.order_id;
                    rows.entry(order_id).or_default().status = Some(status);
                }
                Orders::Notice(notice) => {
                    last_notice = Some(notice.message);
                }
            }
        }
        if let Some(err) = stream.error() {
            return Err(map_ibkr_error(err));
        }
        if let Some(notice) = last_notice {
            for row in rows.values_mut() {
                if row.notice.is_none() {
                    row.notice = Some(notice.clone());
                }
            }
        }

        Ok(rows)
    }

    fn read_live_orders(&self) -> Result<HashMap<i32, OrderSnapshotData>> {
        let mut rows = self.read_orders(|client| client.open_orders())?;
        for (order_id, row) in self.read_orders(|client| client.all_open_orders())? {
            rows.entry(order_id)
                .and_modify(|existing| merge_order_snapshot_row(existing, &row))
                .or_insert(row);
        }
        rows.retain(|_, row| order_row_matches_account(row, &self.settings.account_id));
        Ok(rows)
    }

    fn query_live_order_status(&self, order_id: i32) -> Result<Option<OrderStatusSnapshot>> {
        let orders = self.read_live_orders()?;
        Ok(orders
            .get(&order_id)
            .cloned()
            .map(|row| order_status_snapshot_from_order_row(order_id, row)))
    }

    fn query_completed_order_status(&self, order_id: i32) -> Result<Option<OrderStatusSnapshot>> {
        let orders = self.read_orders(|client| client.completed_orders(false))?;
        Ok(orders
            .get(&order_id)
            .cloned()
            .map(|row| order_status_snapshot_from_order_row(order_id, row)))
    }

    fn query_execution_status(&self, order_id: i32) -> Result<Option<OrderStatusSnapshot>> {
        let client = self.client()?;
        let filter = ExecutionFilter {
            client_id: Some(self.settings.client_id),
            account_code: self.settings.account_id.clone(),
            time: String::new(),
            symbol: String::new(),
            security_type: String::new(),
            exchange: String::new(),
            side: String::new(),
            last_n_days: 7,
            specific_dates: Vec::new(),
        };
        let executions = client.executions(filter).map_err(map_ibkr_error)?;
        let mut latest = None;

        while let Some(update) = executions.next_timeout(SUBSCRIPTION_TIMEOUT) {
            match update {
                Executions::ExecutionData(update) if update.execution.order_id == order_id => {
                    latest = Some(update);
                }
                Executions::ExecutionData(_)
                | Executions::CommissionReport(_)
                | Executions::Notice(_) => {}
            }
        }
        if let Some(err) = executions.error() {
            return Err(map_ibkr_error(err));
        }

        Ok(latest.map(|execution| OrderStatusSnapshot {
            broker_order_id: order_id.to_string(),
            status: "Filled".into(),
            filled_qty: positive_or_none(execution.execution.cumulative_quantity),
            remaining_qty: Some(0.0),
            avg_price: positive_or_none(execution.execution.average_price),
            message: None,
            is_active: false,
            is_final: true,
            raw_metadata: json!({
                "symbol": contract_display_symbol(&execution.contract),
                "broker_order_id": order_id,
                "account": execution.execution.account_number,
                "order_reference": execution.execution.order_reference,
            }),
        }))
    }
}

#[derive(Default, Clone)]
struct OrderSnapshotData {
    data: Option<OrderData>,
    status: Option<IbOrderStatus>,
    notice: Option<String>,
}

#[derive(Default)]
struct PlaceOrderSnapshot {
    data: Option<OrderData>,
    status: Option<IbOrderStatus>,
    execution: Option<ExecutionData>,
    message: Option<String>,
}

fn parse_settings(config: &BrokerConfig) -> Result<IbkrSettings> {
    if !config.settings.is_empty() {
        return Err(TradeBotError::Config(
            "ibkr broker settings now come from environment variables; remove inline settings from config".into(),
        ));
    }

    Ok(IbkrSettings {
        host: optional_env(config, "IBKR_HOST", "HOST").unwrap_or_else(|| "127.0.0.1".into()),
        port: optional_u16_env(config, "IBKR_PORT", "PORT", 4001, "ibkr")?,
        client_id: optional_i32_env(config, "IBKR_CLIENT_ID", "CLIENT_ID", 100, "ibkr")?,
        account_id: required_env(config, "IBKR_ACCOUNT_ID", "ACCOUNT_ID", "ibkr")?,
    })
}

fn build_ib_order(
    client: &Client,
    settings: &IbkrSettings,
    order: &BrokerOrderRequest,
) -> Result<ibapi::orders::Order> {
    let contract = contract_from_resolved(&order.instrument)?;
    let mut builder = client.order(&contract);

    builder = match order.side {
        OrderSide::Buy => builder.buy(order.quantity as f64),
        OrderSide::Sell => builder.sell(order.quantity as f64),
    };

    if order.extended_hours {
        builder = builder.outside_rth();
    }

    let mut ib_order = match order.order_type {
        BrokerOrderType::Market => builder.market().build_order().map_err(map_ibkr_error)?,
        BrokerOrderType::Limit => builder
            .limit(order.limit_price.ok_or_else(|| {
                TradeBotError::Validation(format!(
                    "IBKR limit order for `{}` is missing limit_price",
                    order.instrument.broker_symbol
                ))
            })?)
            .build_order()
            .map_err(map_ibkr_error)?,
    };

    ib_order.account = settings.account_id.clone();
    ib_order.tif = map_time_in_force(order.tif);
    ib_order.order_ref = order
        .client_tag
        .clone()
        .unwrap_or_else(|| order.client_order_id.clone());
    ib_order.outside_rth = order.extended_hours;

    Ok(ib_order)
}

fn collect_place_order_snapshot(
    order_id: i32,
    stream: &mut Subscription<PlaceOrder>,
) -> Result<PlaceOrderSnapshot> {
    let mut snapshot = PlaceOrderSnapshot::default();

    while let Some(update) = stream.next_timeout(SUBSCRIPTION_TIMEOUT) {
        match update {
            PlaceOrder::OpenOrder(data) => snapshot.data = Some(data),
            PlaceOrder::OrderStatus(status) => snapshot.status = Some(status),
            PlaceOrder::ExecutionData(execution) => snapshot.execution = Some(execution),
            PlaceOrder::Message(notice) => snapshot.message = Some(notice.message),
            PlaceOrder::CommissionReport(_) => {}
        }

        if snapshot
            .status
            .as_ref()
            .map(|status| ibkr_status_is_final(&status.status))
            .unwrap_or(false)
        {
            debug!("ibkr order {} reached final state during submit", order_id);
            break;
        }
    }
    if let Some(err) = stream.error() {
        return Err(map_ibkr_error(err));
    }

    Ok(snapshot)
}

fn order_result_from_place_snapshot(
    order_id: i32,
    order: &BrokerOrderRequest,
    snapshot: &PlaceOrderSnapshot,
) -> OrderResult {
    let status = snapshot
        .status
        .as_ref()
        .map(|status| status.status.clone())
        .or_else(|| {
            snapshot
                .data
                .as_ref()
                .and_then(completed_status_from_order_data)
        })
        .unwrap_or_else(|| "Submitted".into());

    OrderResult {
        broker_order_id: order_id.to_string(),
        client_order_id: order.client_order_id.clone(),
        status,
        filled_qty: snapshot
            .status
            .as_ref()
            .and_then(|status| positive_or_none(status.filled))
            .or_else(|| {
                snapshot
                    .execution
                    .as_ref()
                    .and_then(|execution| positive_or_none(execution.execution.cumulative_quantity))
            }),
        avg_price: snapshot
            .status
            .as_ref()
            .and_then(|status| positive_or_none(status.average_fill_price))
            .or_else(|| {
                snapshot
                    .execution
                    .as_ref()
                    .and_then(|execution| positive_or_none(execution.execution.average_price))
            })
            .or(order.limit_price),
        message: snapshot.message.clone(),
        raw_metadata: json!({
            "symbol": order.instrument.broker_symbol,
            "requested_qty": order.quantity,
            "broker_order_id": order_id,
            "account": snapshot
                .data
                .as_ref()
                .map(|data| data.order.account.clone())
                .unwrap_or_default(),
            "order_ref": snapshot
                .data
                .as_ref()
                .map(|data| data.order.order_ref.clone())
                .unwrap_or_else(|| order.client_tag.clone().unwrap_or_else(|| order.client_order_id.clone())),
        }),
    }
}

fn order_status_snapshot_from_order_row(
    order_id: i32,
    row: OrderSnapshotData,
) -> OrderStatusSnapshot {
    let status = row
        .status
        .as_ref()
        .map(|status| status.status.clone())
        .or_else(|| row.data.as_ref().and_then(completed_status_from_order_data))
        .unwrap_or_else(|| "Unknown".into());
    let filled_qty = row
        .status
        .as_ref()
        .and_then(|status| positive_or_none(status.filled));
    let remaining_qty = row
        .status
        .as_ref()
        .and_then(|status| positive_or_none(status.remaining));
    let avg_price = row
        .status
        .as_ref()
        .and_then(|status| positive_or_none(status.average_fill_price));

    let symbol = row
        .data
        .as_ref()
        .map(|data| contract_display_symbol(&data.contract))
        .unwrap_or_default();
    let requested_qty = row
        .data
        .as_ref()
        .and_then(|data| positive_or_none(data.order.total_quantity));

    OrderStatusSnapshot {
        broker_order_id: order_id.to_string(),
        status: status.clone(),
        filled_qty,
        remaining_qty,
        avg_price,
        message: row.notice,
        is_active: !ibkr_status_is_final(&status),
        is_final: ibkr_status_is_final(&status),
        raw_metadata: json!({
            "symbol": symbol,
            "requested_qty": requested_qty,
            "broker_order_id": order_id,
            "account": row
                .data
                .as_ref()
                .map(|data| data.order.account.clone())
                .unwrap_or_default(),
            "order_ref": row
                .data
                .as_ref()
                .map(|data| data.order.order_ref.clone())
                .unwrap_or_default(),
        }),
    }
}

fn merge_order_snapshot_row(existing: &mut OrderSnapshotData, incoming: &OrderSnapshotData) {
    if existing.data.is_none() {
        existing.data = incoming.data.clone();
    }
    if existing.status.is_none() {
        existing.status = incoming.status.clone();
    }
    if existing.notice.is_none() {
        existing.notice = incoming.notice.clone();
    }
}

fn order_row_matches_account(row: &OrderSnapshotData, account_id: &str) -> bool {
    row.data
        .as_ref()
        .map(|data| data.order.account == account_id)
        .unwrap_or(true)
}

fn contract_from_resolved(instrument: &ResolvedInstrument) -> Result<Contract> {
    let symbol = instrument
        .broker_symbol
        .split('.')
        .next()
        .unwrap_or(instrument.broker_symbol.as_str());
    let mut contract = contract_template(symbol, instrument.market);

    if let Some(conid) = instrument.conid.as_deref() {
        contract.contract_id = conid.parse::<i32>().map_err(|_| {
            TradeBotError::Validation(format!(
                "invalid IBKR conid `{conid}` for `{}`",
                instrument.broker_symbol
            ))
        })?;
    }

    Ok(contract)
}

fn contract_template(symbol: &str, market: Market) -> Contract {
    let builder = Contract::stock(symbol)
        .on_exchange(market_exchange(market))
        .in_currency(market_currency(market));

    match market_primary_exchange(market) {
        Some(primary_exchange) => builder.primary(primary_exchange).build(),
        None => builder.build(),
    }
}

fn contract_matches(contract: &Contract, instrument: &ResolvedInstrument) -> bool {
    instrument
        .conid
        .as_deref()
        .and_then(|conid| conid.parse::<i32>().ok())
        .map(|conid| contract.contract_id == conid)
        .unwrap_or_else(|| {
            contract
                .symbol
                .to_string()
                .eq_ignore_ascii_case(&instrument.ticker)
                || contract_display_symbol(contract).eq_ignore_ascii_case(&instrument.broker_symbol)
        })
}

fn contract_display_symbol(contract: &Contract) -> String {
    let local_symbol = contract.local_symbol.to_string();
    if !local_symbol.trim().is_empty() {
        return local_symbol;
    }
    contract.symbol.to_string()
}

fn market_exchange(market: Market) -> &'static str {
    match market {
        Market::Us => "SMART",
        Market::Hk => "SEHK",
    }
}

fn market_primary_exchange(market: Market) -> Option<&'static str> {
    match market {
        Market::Us => None,
        Market::Hk => Some("SEHK"),
    }
}

fn market_currency(market: Market) -> &'static str {
    match market {
        Market::Us => "USD",
        Market::Hk => "HKD",
    }
}

fn normalize_position_qty(quantity: f64) -> Result<u64> {
    if quantity <= 0.0 {
        return Ok(0);
    }
    Ok(quantity.floor() as u64)
}

fn positive_or_none(value: f64) -> Option<f64> {
    (value > 0.0).then_some(value)
}

fn has_quote_value(quote: &Quote) -> bool {
    quote.bid.is_some() || quote.ask.is_some() || quote.last.is_some()
}

fn snapshot_quote(client: &Client, contract: &Contract, market: Market) -> Result<Quote> {
    let stream = client
        .market_data(contract)
        .snapshot()
        .subscribe()
        .map_err(map_ibkr_error)?;

    let mut quote = Quote {
        bid: None,
        ask: None,
        last: None,
        currency: Some(market_currency(market).to_string()),
    };

    while let Some(update) = stream.next_timeout(SUBSCRIPTION_TIMEOUT) {
        match update {
            TickTypes::Price(tick) => apply_quote_tick(&mut quote, tick.tick_type, tick.price),
            TickTypes::PriceSize(tick) => {
                apply_quote_tick(&mut quote, tick.price_tick_type, tick.price)
            }
            _ => {}
        }
    }
    if let Some(err) = stream.error() {
        return Err(map_ibkr_error(err));
    }

    Ok(quote)
}

fn apply_quote_tick(quote: &mut Quote, tick_type: TickType, price: f64) {
    match tick_type {
        TickType::Bid | TickType::DelayedBid => quote.bid = positive_or_none(price),
        TickType::Ask | TickType::DelayedAsk => quote.ask = positive_or_none(price),
        TickType::Last | TickType::DelayedLast => quote.last = positive_or_none(price),
        _ => {}
    }
}

fn completed_status_from_order_data(data: &OrderData) -> Option<String> {
    if !data.order_state.completed_status.is_empty() {
        Some(data.order_state.completed_status.clone())
    } else if !data.order_state.status.is_empty() {
        Some(data.order_state.status.clone())
    } else {
        None
    }
}

fn map_time_in_force(tif: TimeInForce) -> IbTimeInForce {
    match tif {
        TimeInForce::Day => IbTimeInForce::Day,
        TimeInForce::Gtc => IbTimeInForce::GoodTilCanceled,
    }
}

fn parse_order_id(broker_order_id: &str) -> Result<i32> {
    broker_order_id.parse::<i32>().map_err(|_| {
        TradeBotError::Validation(format!(
            "IBKR broker_order_id `{broker_order_id}` is not a valid integer order id"
        ))
    })
}

fn map_ibkr_error(err: ibapi::Error) -> TradeBotError {
    TradeBotError::broker("ibkr", err.to_string())
}

fn required_env(
    config: &BrokerConfig,
    default_name: &str,
    suffix: &str,
    broker: &str,
) -> Result<String> {
    let name = config.env_var_name(default_name, suffix);
    match std::env::var(&name) {
        Ok(value) if !value.trim().is_empty() => Ok(value),
        Ok(_) => Err(TradeBotError::broker(
            broker,
            format!("environment variable `{name}` is set but empty"),
        )),
        Err(_) => Err(TradeBotError::broker(
            broker,
            format!("missing environment variable `{name}`"),
        )),
    }
}

fn optional_env(config: &BrokerConfig, default_name: &str, suffix: &str) -> Option<String> {
    std::env::var(config.env_var_name(default_name, suffix))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn optional_u16_env(
    config: &BrokerConfig,
    default_name: &str,
    suffix: &str,
    default: u16,
    broker: &str,
) -> Result<u16> {
    let name = config.env_var_name(default_name, suffix);
    match std::env::var(&name) {
        Ok(value) => value.trim().parse::<u16>().map_err(|_| {
            TradeBotError::broker(
                broker,
                format!("environment variable `{name}` must be a valid u16"),
            )
        }),
        Err(_) => Ok(default),
    }
}

fn optional_i32_env(
    config: &BrokerConfig,
    default_name: &str,
    suffix: &str,
    default: i32,
    broker: &str,
) -> Result<i32> {
    let name = config.env_var_name(default_name, suffix);
    match std::env::var(&name) {
        Ok(value) => value.trim().parse::<i32>().map_err(|_| {
            TradeBotError::broker(
                broker,
                format!("environment variable `{name}` must be a valid i32"),
            )
        }),
        Err(_) => Ok(default),
    }
}

fn matches_cancel_filter(row: &OrderSnapshotData, request: &CancelRequest) -> bool {
    let Some(data) = &row.data else {
        return false;
    };

    let symbol = contract_display_symbol(&data.contract);
    let conid = data.contract.contract_id.to_string();

    let symbol_match = request.all_open
        || request.instruments.iter().any(|instrument| {
            instrument.ticker.eq_ignore_ascii_case(&symbol)
                || instrument.broker_symbol.eq_ignore_ascii_case(&symbol)
                || instrument
                    .conid
                    .as_deref()
                    .map(|candidate| candidate == conid)
                    .unwrap_or(false)
        });

    let side_match = request.side.map_or(true, |side| {
        data.order
            .action
            .to_string()
            .eq_ignore_ascii_case(side.as_uppercase())
    });

    let tag_match = request
        .client_tag
        .as_ref()
        .map_or(true, |client_tag| data.order.order_ref == *client_tag);

    symbol_match && side_match && tag_match
}

fn ibkr_status_is_final(status: &str) -> bool {
    matches!(
        status.to_ascii_lowercase().as_str(),
        "filled"
            | "cancelled"
            | "canceled"
            | "inactive"
            | "rejected"
            | "expired"
            | "apicancelled"
            | "apicanceled"
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use trading_core::BrokerConfig;

    use super::{optional_i32_env, optional_u16_env, required_env};

    #[test]
    fn resolves_legacy_ibkr_env_names_without_prefix() {
        let config = BrokerConfig {
            kind: "ibkr".into(),
            env_prefix: None,
            settings: BTreeMap::new(),
        };

        let err = required_env(&config, "IBKR_ACCOUNT_ID", "ACCOUNT_ID", "ibkr").unwrap_err();
        assert!(err.to_string().contains("`IBKR_ACCOUNT_ID`"));
    }

    #[test]
    fn resolves_prefixed_ibkr_env_names() {
        let config = BrokerConfig {
            kind: "ibkr".into(),
            env_prefix: Some("IBKR_MAIN".into()),
            settings: BTreeMap::new(),
        };

        let err = required_env(&config, "IBKR_ACCOUNT_ID", "ACCOUNT_ID", "ibkr").unwrap_err();
        assert!(err.to_string().contains("`IBKR_MAIN_ACCOUNT_ID`"));
    }

    #[test]
    fn uses_defaults_for_missing_prefixed_optional_envs() {
        let config = BrokerConfig {
            kind: "ibkr".into(),
            env_prefix: Some("IBKR_MAIN".into()),
            settings: BTreeMap::new(),
        };

        assert_eq!(
            optional_u16_env(&config, "IBKR_PORT", "PORT", 4001, "ibkr").unwrap(),
            4001
        );
        assert_eq!(
            optional_i32_env(&config, "IBKR_CLIENT_ID", "CLIENT_ID", 100, "ibkr").unwrap(),
            100
        );
    }
}
