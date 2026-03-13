use std::env;

use serde::Deserialize;
use trading_core::{
    Broker, BrokerConfig, BrokerFactory, BrokerHealth, BrokerOrderRequest, CancelRequest,
    CancelResult, InstrumentRef, OrderResult, Quote, ResolvedInstrument, Result, TradeBotError,
};

#[derive(Default)]
pub struct LongbridgeBrokerFactory;

impl BrokerFactory for LongbridgeBrokerFactory {
    fn kind(&self) -> &'static str {
        "longbridge"
    }

    fn build(&self, broker_name: &str, config: &BrokerConfig) -> Result<Box<dyn Broker>> {
        let settings = parse_settings(config)?;
        Ok(Box::new(LongbridgeBroker {
            name: broker_name.to_string(),
            settings,
        }))
    }
}

#[derive(Debug, Clone, Deserialize)]
struct LongbridgeSettings {
    app_key_env: String,
    app_secret_env: String,
    access_token_env: String,
}

struct LongbridgeBroker {
    name: String,
    settings: LongbridgeSettings,
}

impl Broker for LongbridgeBroker {
    fn broker_name(&self) -> &str {
        &self.name
    }

    fn broker_kind(&self) -> &str {
        "longbridge"
    }

    fn health_check(&self) -> Result<BrokerHealth> {
        let app_key = env::var(&self.settings.app_key_env).ok();
        let app_secret = env::var(&self.settings.app_secret_env).ok();
        let access_token = env::var(&self.settings.access_token_env).ok();
        let configured = app_key.is_some() && app_secret.is_some() && access_token.is_some();

        Ok(BrokerHealth {
            reachable: configured,
            authenticated: configured,
            brokerage_session: configured,
            message: if configured {
                None
            } else {
                Some("missing Longbridge credential environment variables".into())
            },
        })
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

    fn fetch_quote(&self, _instrument: &ResolvedInstrument) -> Result<Quote> {
        Err(TradeBotError::Unsupported(
            "Longbridge quote fetching is not wired yet; use explicit quantity with limit pricing for now".into(),
        ))
    }

    fn place_orders(&self, _orders: &[BrokerOrderRequest]) -> Result<Vec<OrderResult>> {
        Err(TradeBotError::Unsupported(
            "Longbridge live order placement is scaffolded but not wired to the SDK yet".into(),
        ))
    }

    fn cancel_orders(&self, _request: &CancelRequest) -> Result<Vec<CancelResult>> {
        Err(TradeBotError::Unsupported(
            "Longbridge live cancel flow is scaffolded but not wired to the SDK yet".into(),
        ))
    }
}

fn parse_settings(config: &BrokerConfig) -> Result<LongbridgeSettings> {
    let value = toml::Value::Table(config.settings.clone().into_iter().collect());
    value
        .try_into()
        .map_err(|err| TradeBotError::Config(format!("invalid longbridge settings: {err}")))
}
