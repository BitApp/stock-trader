use serde_json::Value;
use uuid::Uuid;

use crate::{
    config::{DefaultsConfig, TaskConfig},
    errors::{Result, TradeBotError},
    models::{
        BrokerOrderRequest, BrokerOrderType, InstrumentRef, OrderSide, PositionSnapshot,
        PricingSpec, Quote, ResolvedInstrument,
    },
};

pub fn materialize_orders<F, G>(
    task: &TaskConfig,
    defaults: &DefaultsConfig,
    mut resolve: F,
    mut fetch_position: impl FnMut(&ResolvedInstrument) -> Result<PositionSnapshot>,
    mut fetch_quote: G,
) -> Result<Vec<BrokerOrderRequest>>
where
    F: FnMut(&InstrumentRef) -> Result<ResolvedInstrument>,
    G: FnMut(&ResolvedInstrument) -> Result<Quote>,
{
    let side = task.side.ok_or_else(|| {
        TradeBotError::Validation(format!("task `{}` is missing side", task.name))
    })?;
    let pricing = task.pricing.clone().ok_or_else(|| {
        TradeBotError::Validation(format!("task `{}` is missing pricing", task.name))
    })?;
    let tif = task.time_in_force.unwrap_or(defaults.default_tif);
    let allow_margin = task
        .risk
        .as_ref()
        .map(|risk| risk.allow_margin)
        .unwrap_or(false);
    let extended_hours = task
        .session
        .as_ref()
        .map(|session| session.extended_hours)
        .unwrap_or(false);

    let mut resolved = Vec::with_capacity(task.symbols.len());
    for symbol in &task.symbols {
        resolved.push((symbol, resolve(&symbol.instrument)?));
    }

    let weight_total: f64 = task
        .symbols
        .iter()
        .map(|symbol| symbol.weight.unwrap_or(0.0))
        .sum();

    let mut output = Vec::with_capacity(task.symbols.len());

    for (symbol, resolved_instrument) in resolved {
        let reference_price = estimate_reference_price(
            &pricing,
            side,
            symbol.limit_price,
            &resolved_instrument,
            &mut fetch_quote,
        )?;

        let quantity = if symbol.close_position {
            fetch_position_quantity(
                task,
                &symbol.instrument.ticker,
                &resolved_instrument,
                &mut fetch_position,
            )?
        } else if let Some(quantity) = symbol.quantity {
            quantity
        } else if let Some(amount) = symbol.amount {
            amount_to_quantity(amount, reference_price, &symbol.instrument.ticker)?
        } else if let Some(shared_budget) = &task.shared_budget {
            let weight = symbol.weight.ok_or_else(|| {
                TradeBotError::Validation(format!(
                    "task `{}` symbol `{}` missing weight",
                    task.name, symbol.instrument.ticker
                ))
            })?;
            let allocation = shared_budget.amount * (weight / weight_total);
            amount_to_quantity(allocation, reference_price, &symbol.instrument.ticker)?
        } else {
            return Err(TradeBotError::Validation(format!(
                "task `{}` symbol `{}` has no allocation",
                task.name, symbol.instrument.ticker
            )));
        };

        let order_type = match pricing {
            PricingSpec::Market => BrokerOrderType::Market,
            PricingSpec::Limit { .. } | PricingSpec::Counterparty => BrokerOrderType::Limit,
        };

        let limit_price =
            effective_limit_price(&pricing, side, symbol.limit_price, reference_price);
        let client_order_id = symbol.client_order_id.clone().unwrap_or_else(|| {
            build_client_order_id(&task.name, &resolved_instrument.broker_symbol)
        });

        output.push(BrokerOrderRequest {
            instrument: resolved_instrument,
            side,
            order_type,
            quantity,
            limit_price,
            tif,
            allow_margin,
            extended_hours,
            client_order_id,
            client_tag: task.client_tag.clone(),
            raw_metadata: Value::Null,
        });
    }

    Ok(output)
}

fn fetch_position_quantity<F>(
    task: &TaskConfig,
    ticker: &str,
    resolved_instrument: &ResolvedInstrument,
    fetch_position: &mut F,
) -> Result<u64>
where
    F: FnMut(&ResolvedInstrument) -> Result<PositionSnapshot>,
{
    let position = fetch_position(resolved_instrument)?;
    if position.available_quantity == 0 {
        return Err(TradeBotError::Validation(format!(
            "task `{}` symbol `{ticker}` has no available position to close",
            task.name
        )));
    }
    Ok(position.available_quantity)
}

pub fn estimate_reference_price<G>(
    pricing: &PricingSpec,
    side: OrderSide,
    symbol_limit_price: Option<f64>,
    resolved_instrument: &ResolvedInstrument,
    fetch_quote: &mut G,
) -> Result<f64>
where
    G: FnMut(&ResolvedInstrument) -> Result<Quote>,
{
    match pricing {
        PricingSpec::Limit { price } => price.or(symbol_limit_price).ok_or_else(|| {
            TradeBotError::Validation(format!(
                "symbol `{}` is missing a limit price",
                resolved_instrument.broker_symbol
            ))
        }),
        PricingSpec::Counterparty => {
            let quote = fetch_quote(resolved_instrument)?;
            let selected = match side {
                OrderSide::Buy => quote.ask,
                OrderSide::Sell => quote.bid,
            };
            selected.ok_or_else(|| {
                TradeBotError::Validation(format!(
                    "missing counterparty quote for `{}`",
                    resolved_instrument.broker_symbol
                ))
            })
        }
        PricingSpec::Market => {
            let quote = fetch_quote(resolved_instrument)?;
            match side {
                OrderSide::Buy => quote.ask.or(quote.last),
                OrderSide::Sell => quote.bid.or(quote.last),
            }
            .ok_or_else(|| {
                TradeBotError::Validation(format!(
                    "missing quote for market order quantity estimation on `{}`",
                    resolved_instrument.broker_symbol
                ))
            })
        }
    }
}

fn effective_limit_price(
    pricing: &PricingSpec,
    side: OrderSide,
    symbol_limit_price: Option<f64>,
    reference_price: f64,
) -> Option<f64> {
    match pricing {
        PricingSpec::Market => None,
        PricingSpec::Limit { price } => price.or(symbol_limit_price),
        PricingSpec::Counterparty => Some(match side {
            OrderSide::Buy => reference_price,
            OrderSide::Sell => reference_price,
        }),
    }
}

fn amount_to_quantity(amount: f64, reference_price: f64, ticker: &str) -> Result<u64> {
    if amount <= 0.0 {
        return Err(TradeBotError::Validation(format!(
            "allocation amount for `{ticker}` must be positive"
        )));
    }
    if reference_price <= 0.0 {
        return Err(TradeBotError::Validation(format!(
            "reference price for `{ticker}` must be positive"
        )));
    }
    let quantity = (amount / reference_price).floor() as u64;
    if quantity == 0 {
        return Err(TradeBotError::Validation(format!(
            "allocation for `{ticker}` rounds down to zero quantity"
        )));
    }
    Ok(quantity)
}

fn build_client_order_id(task_name: &str, broker_symbol: &str) -> String {
    format!(
        "{}-{}-{}",
        task_name,
        broker_symbol.replace('.', "-"),
        Uuid::new_v4().simple()
    )
}

#[cfg(test)]
mod tests {
    use crate::models::{Market, PositionSnapshot, PricingSpec, Quote, ResolvedInstrument};

    use super::*;

    fn resolved(symbol: &str) -> ResolvedInstrument {
        ResolvedInstrument {
            ticker: symbol.to_string(),
            market: Market::Us,
            broker_symbol: format!("{symbol}.US"),
            conid: Some("123".into()),
        }
    }

    #[test]
    fn counterparty_uses_ask_for_buy() {
        let mut fetch = |_symbol: &ResolvedInstrument| {
            Ok(Quote {
                bid: Some(99.0),
                ask: Some(101.0),
                last: Some(100.0),
                currency: None,
            })
        };
        let price = estimate_reference_price(
            &PricingSpec::Counterparty,
            OrderSide::Buy,
            None,
            &resolved("AAPL"),
            &mut fetch,
        )
        .unwrap();
        assert_eq!(price, 101.0);
    }

    #[test]
    fn market_sell_prefers_bid() {
        let mut fetch = |_symbol: &ResolvedInstrument| {
            Ok(Quote {
                bid: Some(89.0),
                ask: Some(90.0),
                last: Some(89.5),
                currency: None,
            })
        };
        let price = estimate_reference_price(
            &PricingSpec::Market,
            OrderSide::Sell,
            None,
            &resolved("TSLA"),
            &mut fetch,
        )
        .unwrap();
        assert_eq!(price, 89.0);
    }

    #[test]
    fn close_position_uses_available_quantity() {
        let task = TaskConfig {
            name: "close-spy".into(),
            broker: "paper".into(),
            action: crate::models::TaskAction::Place,
            schedule: None,
            execution: None,
            notify: None,
            side: Some(OrderSide::Sell),
            pricing: Some(PricingSpec::Market),
            risk: None,
            session: Some(crate::models::SessionPolicy {
                extended_hours: true,
            }),
            shared_budget: None,
            time_in_force: None,
            client_tag: None,
            all_open: false,
            symbols: vec![crate::models::SymbolTarget {
                instrument: InstrumentRef {
                    ticker: "SPY".into(),
                    market: Market::Us,
                    broker_symbol: None,
                    conid: None,
                },
                close_position: true,
                quantity: None,
                amount: None,
                weight: None,
                limit_price: None,
                client_order_id: None,
                broker_options: std::collections::BTreeMap::new(),
            }],
        };
        let defaults = DefaultsConfig::default();

        let orders = materialize_orders(
            &task,
            &defaults,
            |instrument| {
                Ok(ResolvedInstrument {
                    ticker: instrument.ticker.clone(),
                    market: instrument.market,
                    broker_symbol: "SPY.US".into(),
                    conid: None,
                })
            },
            |_instrument| {
                Ok(PositionSnapshot {
                    instrument: resolved("SPY"),
                    quantity: 15,
                    available_quantity: 12,
                })
            },
            |_instrument| {
                Ok(Quote {
                    bid: Some(500.0),
                    ask: Some(500.5),
                    last: Some(500.2),
                    currency: None,
                })
            },
        )
        .unwrap();

        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].quantity, 12);
        assert_eq!(orders[0].order_type, crate::models::BrokerOrderType::Market);
        assert!(orders[0].extended_hours);
    }
}
