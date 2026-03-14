use std::{env, sync::Arc};

use longport::{
    Config as LongportConfig, Decimal,
    blocking::{QuoteContextSync, TradeContextSync},
    trade::{
        GetStockPositionsOptions, GetTodayOrdersOptions, OrderDetail as LongportOrderDetail,
        OrderSide as LongportOrderSide, OrderStatus as LongportOrderStatus,
        OrderType as LongportOrderType, OutsideRTH as LongportOutsideRTH, SubmitOrderOptions,
        TimeInForceType as LongportTimeInForceType,
    },
};
use serde_json::json;
use trading_core::{
    Broker, BrokerConfig, BrokerFactory, BrokerHealth, BrokerOrderRequest, BrokerOrderType,
    CancelRequest, CancelResult, InstrumentRef, OrderResult, OrderStatusSnapshot, PositionSnapshot,
    Quote, ResolvedInstrument, Result, TradeBotError,
};

#[derive(Default)]
pub struct LongbridgeBrokerFactory;

impl BrokerFactory for LongbridgeBrokerFactory {
    fn kind(&self) -> &'static str {
        "longbridge"
    }

    fn build(&self, broker_name: &str, config: &BrokerConfig) -> Result<Box<dyn Broker>> {
        validate_settings(config)?;
        let (trade, quote, init_error) = match build_contexts(config) {
            Ok((trade, quote)) => (Some(trade), Some(quote), None),
            Err(err) => (None, None, Some(err.to_string())),
        };

        Ok(Box::new(LongbridgeBroker {
            name: broker_name.to_string(),
            trade,
            quote,
            init_error,
        }))
    }
}

struct LongbridgeBroker {
    name: String,
    trade: Option<TradeContextSync>,
    quote: Option<QuoteContextSync>,
    init_error: Option<String>,
}

impl Broker for LongbridgeBroker {
    fn broker_name(&self) -> &str {
        &self.name
    }

    fn broker_kind(&self) -> &str {
        "longbridge"
    }

    fn health_check(&self) -> Result<BrokerHealth> {
        let Some(trade) = &self.trade else {
            return Ok(BrokerHealth {
                reachable: false,
                authenticated: false,
                brokerage_session: false,
                message: self.init_error.clone(),
            });
        };

        match trade.account_balance(None) {
            Ok(_) => Ok(BrokerHealth {
                reachable: true,
                authenticated: true,
                brokerage_session: true,
                message: None,
            }),
            Err(err) => Ok(BrokerHealth {
                reachable: false,
                authenticated: false,
                brokerage_session: false,
                message: Some(err.to_string()),
            }),
        }
    }

    fn resolve_instrument(&self, instrument: &InstrumentRef) -> Result<ResolvedInstrument> {
        let broker_symbol = instrument.broker_symbol.clone().unwrap_or_else(|| {
            format!(
                "{}.{}",
                instrument.ticker,
                instrument.market.longbridge_suffix()
            )
        });

        Ok(ResolvedInstrument {
            ticker: instrument.ticker.clone(),
            market: instrument.market,
            broker_symbol,
            conid: instrument.conid.clone(),
        })
    }

    fn fetch_position(&self, instrument: &ResolvedInstrument) -> Result<PositionSnapshot> {
        let trade = self.trade_context()?;
        let broker_symbol = instrument.broker_symbol.clone();
        let response = trade
            .stock_positions(Some(
                GetStockPositionsOptions::new().symbols([broker_symbol.clone()]),
            ))
            .map_err(map_longbridge_error)?;

        let position = response
            .channels
            .into_iter()
            .flat_map(|channel| channel.positions.into_iter())
            .find(|position| position.symbol == broker_symbol)
            .ok_or_else(|| {
                TradeBotError::Validation(format!(
                    "Longbridge position not found for `{}`",
                    instrument.broker_symbol
                ))
            })?;

        Ok(PositionSnapshot {
            instrument: instrument.clone(),
            quantity: decimal_to_u64(position.quantity, "quantity", &instrument.broker_symbol)?,
            available_quantity: decimal_to_u64(
                position.available_quantity,
                "available_quantity",
                &instrument.broker_symbol,
            )?,
        })
    }

    fn fetch_quote(&self, instrument: &ResolvedInstrument) -> Result<Quote> {
        let broker_symbol = instrument.broker_symbol.clone();
        let quote = self
            .quote_context()?
            .quote([broker_symbol.clone()])
            .map_err(map_longbridge_error)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                TradeBotError::broker(
                    "longbridge",
                    format!("missing quote for `{}`", instrument.broker_symbol),
                )
            })?;

        Ok(Quote {
            bid: None,
            ask: None,
            last: Some(decimal_to_f64(
                quote.last_done,
                "last_done",
                &instrument.broker_symbol,
            )?),
            currency: None,
        })
    }

    fn place_orders(&self, orders: &[BrokerOrderRequest]) -> Result<Vec<OrderResult>> {
        let trade = self.trade_context()?;

        orders
            .iter()
            .map(|order| {
                let mut options = SubmitOrderOptions::new(
                    order.instrument.broker_symbol.as_str(),
                    match order.order_type {
                        BrokerOrderType::Market => LongportOrderType::MO,
                        BrokerOrderType::Limit => LongportOrderType::LO,
                    },
                    match order.side {
                        trading_core::OrderSide::Buy => LongportOrderSide::Buy,
                        trading_core::OrderSide::Sell => LongportOrderSide::Sell,
                    },
                    Decimal::from(order.quantity),
                    match order.tif {
                        trading_core::TimeInForce::Day => LongportTimeInForceType::Day,
                        trading_core::TimeInForce::Gtc => LongportTimeInForceType::GoodTilCanceled,
                    },
                );

                if let Some(limit_price) = order.limit_price {
                    options = options.submitted_price(f64_to_decimal(
                        limit_price,
                        "limit_price",
                        &order.instrument.broker_symbol,
                    )?);
                }
                if order.extended_hours {
                    options = options.outside_rth(LongportOutsideRTH::AnyTime);
                }

                let remark = order
                    .client_tag
                    .as_deref()
                    .unwrap_or(&order.client_order_id)
                    .to_string();
                options = options.remark(remark);

                let response = trade.submit_order(options).map_err(map_longbridge_error)?;

                Ok(OrderResult {
                    broker_order_id: response.order_id,
                    client_order_id: order.client_order_id.clone(),
                    status: "submitted".into(),
                    filled_qty: None,
                    avg_price: order.limit_price,
                    message: None,
                    raw_metadata: json!({ "symbol": order.instrument.broker_symbol }),
                })
            })
            .collect()
    }

    fn cancel_orders(&self, _request: &CancelRequest) -> Result<Vec<CancelResult>> {
        let trade = self.trade_context()?;
        let live_orders = trade
            .today_orders(None::<GetTodayOrdersOptions>)
            .map_err(map_longbridge_error)?;

        live_orders
            .into_iter()
            .filter(|order| longbridge_order_matches_cancel_filter(order, _request))
            .map(|order| self.cancel_order_by_id(&order.order_id))
            .collect()
    }

    fn cancel_order_by_id(&self, broker_order_id: &str) -> Result<CancelResult> {
        self.trade_context()?
            .cancel_order(broker_order_id.to_string())
            .map_err(map_longbridge_error)?;

        Ok(CancelResult {
            broker_order_id: broker_order_id.to_string(),
            status: "cancel_submitted".into(),
            message: None,
            raw_metadata: json!({}),
        })
    }

    fn get_order_status(&self, broker_order_id: &str) -> Result<OrderStatusSnapshot> {
        let detail = self
            .trade_context()?
            .order_detail(broker_order_id.to_string())
            .map_err(map_longbridge_error)?;
        Ok(order_snapshot_from_detail(detail))
    }
}

impl LongbridgeBroker {
    fn trade_context(&self) -> Result<&TradeContextSync> {
        self.trade.as_ref().ok_or_else(|| {
            TradeBotError::broker(
                "longbridge",
                self.init_error
                    .clone()
                    .unwrap_or_else(|| "Longbridge trade context is unavailable".into()),
            )
        })
    }

    fn quote_context(&self) -> Result<&QuoteContextSync> {
        self.quote.as_ref().ok_or_else(|| {
            TradeBotError::broker(
                "longbridge",
                self.init_error
                    .clone()
                    .unwrap_or_else(|| "Longbridge quote context is unavailable".into()),
            )
        })
    }
}

fn build_contexts(config: &BrokerConfig) -> Result<(TradeContextSync, QuoteContextSync)> {
    let config = Arc::new(build_sdk_config(config)?);
    let trade = TradeContextSync::try_new(config.clone(), |_| ()).map_err(map_longbridge_error)?;
    let quote = QuoteContextSync::try_new(config, |_| ()).map_err(map_longbridge_error)?;
    Ok((trade, quote))
}

fn build_sdk_config(config: &BrokerConfig) -> Result<LongportConfig> {
    let app_key = required_env(config, "LONGPORT_APP_KEY", "APP_KEY").map_err(|name| {
        TradeBotError::broker(
            "longbridge",
            format!("missing Longbridge environment variable `{name}`"),
        )
    })?;
    let app_secret = required_env(config, "LONGPORT_APP_SECRET", "APP_SECRET").map_err(|name| {
        TradeBotError::broker(
            "longbridge",
            format!("missing Longbridge environment variable `{name}`"),
        )
    })?;
    let access_token =
        required_env(config, "LONGPORT_ACCESS_TOKEN", "ACCESS_TOKEN").map_err(|name| {
            TradeBotError::broker(
                "longbridge",
                format!("missing Longbridge environment variable `{name}`"),
            )
        })?;

    Ok(LongportConfig::new(app_key, app_secret, access_token).dont_print_quote_packages())
}

fn validate_settings(config: &BrokerConfig) -> Result<()> {
    if config.settings.is_empty() {
        return Ok(());
    }

    Err(TradeBotError::Config(
        "longbridge broker settings now come from environment variables; remove inline settings from config".into(),
    ))
}

fn required_env(
    config: &BrokerConfig,
    default_name: &str,
    suffix: &str,
) -> std::result::Result<String, String> {
    let name = config.env_var_name(default_name, suffix);
    env::var(&name).map_err(|_| name)
}

fn map_longbridge_error(err: impl ToString) -> TradeBotError {
    TradeBotError::broker("longbridge", err.to_string())
}

fn decimal_to_u64(value: Decimal, field: &str, symbol: &str) -> Result<u64> {
    let normalized = value.normalize().to_string();
    normalized.parse::<u64>().map_err(|_| {
        TradeBotError::broker(
            "longbridge",
            format!("expected integer {field} for `{symbol}`, got `{normalized}` from Longbridge"),
        )
    })
}

fn decimal_to_f64(value: Decimal, field: &str, symbol: &str) -> Result<f64> {
    value.to_string().parse::<f64>().map_err(|_| {
        TradeBotError::broker(
            "longbridge",
            format!("failed to parse {field} for `{symbol}` from Longbridge quote"),
        )
    })
}

fn f64_to_decimal(value: f64, field: &str, symbol: &str) -> Result<Decimal> {
    value.to_string().parse::<Decimal>().map_err(|_| {
        TradeBotError::Validation(format!("invalid {field} for `{symbol}`: `{value}`"))
    })
}

fn decimal_to_plain_f64(value: Decimal) -> std::result::Result<f64, std::num::ParseFloatError> {
    value.to_string().parse::<f64>()
}

fn order_snapshot_from_detail(detail: LongportOrderDetail) -> OrderStatusSnapshot {
    let filled_qty = decimal_to_plain_f64(detail.executed_quantity).ok();
    let total_qty = decimal_to_plain_f64(detail.quantity).ok();
    let remaining_qty = total_qty
        .zip(filled_qty)
        .map(|(total, filled)| (total - filled).max(0.0));
    let status = detail.status.to_string();
    let is_final = matches!(
        detail.status,
        LongportOrderStatus::Filled
            | LongportOrderStatus::Canceled
            | LongportOrderStatus::Expired
            | LongportOrderStatus::Rejected
            | LongportOrderStatus::PartialWithdrawal
    );

    OrderStatusSnapshot {
        broker_order_id: detail.order_id,
        status,
        filled_qty,
        remaining_qty,
        avg_price: detail
            .executed_price
            .and_then(|value| decimal_to_plain_f64(value).ok()),
        message: (!detail.msg.is_empty()).then_some(detail.msg),
        is_active: !is_final,
        is_final,
        raw_metadata: json!({ "symbol": detail.symbol }),
    }
}

fn longbridge_order_matches_cancel_filter(
    order: &longport::trade::Order,
    request: &CancelRequest,
) -> bool {
    if matches!(
        order.status,
        LongportOrderStatus::Filled
            | LongportOrderStatus::Canceled
            | LongportOrderStatus::Expired
            | LongportOrderStatus::Rejected
            | LongportOrderStatus::PartialWithdrawal
    ) {
        return false;
    }

    let symbol_match = request.all_open
        || request
            .instruments
            .iter()
            .any(|instrument| instrument.broker_symbol.eq_ignore_ascii_case(&order.symbol));

    let side_match = request.side.map_or(true, |side| match side {
        trading_core::OrderSide::Buy => order.side == LongportOrderSide::Buy,
        trading_core::OrderSide::Sell => order.side == LongportOrderSide::Sell,
    });

    let tag_match = request
        .client_tag
        .as_ref()
        .map_or(true, |tag| order.remark == *tag || order.msg.contains(tag));

    symbol_match && side_match && tag_match
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use trading_core::BrokerConfig;

    use super::required_env;

    #[test]
    fn resolves_legacy_longbridge_env_names_without_prefix() {
        let config = BrokerConfig {
            kind: "longbridge".into(),
            env_prefix: None,
            settings: BTreeMap::new(),
        };

        let err = required_env(&config, "LONGPORT_APP_KEY", "APP_KEY").unwrap_err();
        assert_eq!(err, "LONGPORT_APP_KEY");
    }

    #[test]
    fn resolves_prefixed_longbridge_env_names() {
        let config = BrokerConfig {
            kind: "longbridge".into(),
            env_prefix: Some("LONGPORT_MAIN".into()),
            settings: BTreeMap::new(),
        };

        let err = required_env(&config, "LONGPORT_APP_KEY", "APP_KEY").unwrap_err();
        assert_eq!(err, "LONGPORT_MAIN_APP_KEY");
    }
}
