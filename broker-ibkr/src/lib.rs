use std::time::Duration;

use reqwest::blocking::{Client, ClientBuilder};
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::debug;
use trading_core::{
    Broker, BrokerConfig, BrokerFactory, BrokerHealth, BrokerOrderRequest, BrokerOrderType,
    CancelRequest, CancelResult, InstrumentRef, OrderResult, Quote, ResolvedInstrument, Result,
    TradeBotError,
};

#[derive(Default)]
pub struct IbkrBrokerFactory;

impl BrokerFactory for IbkrBrokerFactory {
    fn kind(&self) -> &'static str {
        "ibkr"
    }

    fn build(&self, broker_name: &str, config: &BrokerConfig) -> Result<Box<dyn Broker>> {
        let settings = parse_settings(config)?;
        let client = ClientBuilder::new()
            .danger_accept_invalid_certs(settings.allow_insecure_tls)
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|err| TradeBotError::broker("ibkr", err.to_string()))?;

        Ok(Box::new(IbkrBroker {
            name: broker_name.to_string(),
            settings,
            client,
        }))
    }
}

#[derive(Debug, Clone, Deserialize)]
struct IbkrSettings {
    base_url: String,
    account_id: String,
    #[serde(default)]
    allow_insecure_tls: bool,
    #[serde(default = "default_true")]
    auto_confirm_replies: bool,
}

fn default_true() -> bool {
    true
}

struct IbkrBroker {
    name: String,
    settings: IbkrSettings,
    client: Client,
}

impl Broker for IbkrBroker {
    fn broker_name(&self) -> &str {
        &self.name
    }

    fn broker_kind(&self) -> &str {
        "ibkr"
    }

    fn health_check(&self) -> Result<BrokerHealth> {
        let auth = match self.get_json::<Value>("iserver/auth/status") {
            Ok(auth) => auth,
            Err(err) => {
                return Ok(BrokerHealth {
                    reachable: false,
                    authenticated: false,
                    brokerage_session: false,
                    message: Some(err.to_string()),
                });
            }
        };

        let brokerage_session = self
            .get_json::<Value>("iserver/accounts")
            .map(|accounts| {
                accounts
                    .as_array()
                    .map(|rows| !rows.is_empty())
                    .unwrap_or(false)
            })
            .unwrap_or(false);

        Ok(BrokerHealth {
            reachable: true,
            authenticated: auth
                .get("authenticated")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            brokerage_session,
            message: auth
                .get("message")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        })
    }

    fn resolve_instrument(&self, instrument: &InstrumentRef) -> Result<ResolvedInstrument> {
        let conid = instrument.conid.clone().ok_or_else(|| {
            TradeBotError::Validation(format!(
                "IBKR instrument `{}` requires explicit conid in v1",
                instrument.ticker
            ))
        })?;

        Ok(ResolvedInstrument {
            ticker: instrument.ticker.clone(),
            market: instrument.market,
            broker_symbol: instrument
                .broker_symbol
                .clone()
                .unwrap_or_else(|| instrument.ticker.clone()),
            conid: Some(conid),
        })
    }

    fn fetch_quote(&self, instrument: &ResolvedInstrument) -> Result<Quote> {
        let conid = instrument
            .conid
            .as_ref()
            .ok_or_else(|| TradeBotError::Validation("IBKR quote lookup requires conid".into()))?;
        let snapshot_path =
            format!("iserver/marketdata/snapshot?conids={conid}&fields=31,84,85,86,88");

        let _preflight: Value = self.get_json(&snapshot_path)?;
        let payload: Value = self.get_json(&snapshot_path)?;
        let first = payload
            .as_array()
            .and_then(|rows| rows.first())
            .ok_or_else(|| TradeBotError::broker("ibkr", "empty snapshot response"))?;

        Ok(Quote {
            bid: parse_ibkr_number(first.get("84")),
            ask: parse_ibkr_number(first.get("86")),
            last: parse_ibkr_number(first.get("31")),
            currency: None,
        })
    }

    fn place_orders(&self, orders: &[BrokerOrderRequest]) -> Result<Vec<OrderResult>> {
        let payload = orders
            .iter()
            .map(|order| {
                let mut body = json!({
                    "conid": parse_conid_value(order.instrument.conid.as_deref()),
                    "side": order.side.as_uppercase(),
                    "orderType": match order.order_type {
                        BrokerOrderType::Market => "MKT",
                        BrokerOrderType::Limit => "LMT",
                    },
                    "quantity": order.quantity,
                    "tif": order.tif.as_ibkr(),
                    "order_ref": order.client_order_id,
                });
                if let Some(limit_price) = order.limit_price {
                    body["price"] = json!(limit_price);
                }
                if order.allow_margin {
                    body["isClose"] = json!(false);
                }
                body
            })
            .collect::<Vec<_>>();

        let response = self.post_json(
            &format!("iserver/account/{}/orders", self.settings.account_id),
            &Value::Array(payload),
        )?;

        orders
            .iter()
            .map(|order| self.parse_place_response(&response, order))
            .collect()
    }

    fn cancel_orders(&self, request: &CancelRequest) -> Result<Vec<CancelResult>> {
        let live_orders = self.get_json::<Value>(&format!(
            "iserver/account/orders?force=true&accountId={}",
            self.settings.account_id
        ))?;
        let rows = live_orders
            .get("orders")
            .and_then(Value::as_array)
            .ok_or_else(|| TradeBotError::broker("ibkr", "invalid live orders response"))?;

        let mut results = Vec::new();

        for row in rows {
            if !matches_cancel_filter(row, request) {
                continue;
            }
            let order_id = row
                .get("orderId")
                .and_then(value_to_string)
                .ok_or_else(|| TradeBotError::broker("ibkr", "missing orderId"))?;
            let response = self.delete_json(&format!(
                "iserver/account/{}/order/{}",
                self.settings.account_id, order_id
            ))?;

            results.push(CancelResult {
                broker_order_id: order_id,
                status: response
                    .get("msg")
                    .and_then(Value::as_str)
                    .unwrap_or("cancel_submitted")
                    .to_string(),
                message: response
                    .get("msg")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                raw_metadata: response,
            });
        }

        Ok(results)
    }
}

impl IbkrBroker {
    fn parse_place_response(
        &self,
        response: &Value,
        order: &BrokerOrderRequest,
    ) -> Result<OrderResult> {
        if response.is_object() {
            return Ok(OrderResult {
                broker_order_id: response
                    .get("order_id")
                    .and_then(value_to_string)
                    .unwrap_or_else(|| "unknown".into()),
                client_order_id: order.client_order_id.clone(),
                status: response
                    .get("order_status")
                    .and_then(Value::as_str)
                    .unwrap_or("submitted")
                    .to_string(),
                filled_qty: None,
                avg_price: order.limit_price,
                message: None,
                raw_metadata: response.clone(),
            });
        }

        let first_reply = response
            .as_array()
            .and_then(|rows| rows.first())
            .ok_or_else(|| TradeBotError::broker("ibkr", "invalid order response"))?;

        let reply_id = first_reply
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| TradeBotError::broker("ibkr", "missing IBKR reply id"))?;

        let ack = if self.settings.auto_confirm_replies {
            self.post_json(
                &format!("iserver/reply/{reply_id}"),
                &json!({ "confirmed": true }),
            )?
        } else {
            return Err(TradeBotError::broker(
                "ibkr",
                format!("order reply requires confirmation: {}", first_reply),
            ));
        };

        Ok(OrderResult {
            broker_order_id: ack
                .get("order_id")
                .and_then(value_to_string)
                .unwrap_or_else(|| "unknown".into()),
            client_order_id: order.client_order_id.clone(),
            status: ack
                .get("order_status")
                .and_then(Value::as_str)
                .unwrap_or("submitted")
                .to_string(),
            filled_qty: None,
            avg_price: order.limit_price,
            message: first_reply
                .get("message")
                .and_then(Value::as_array)
                .and_then(|messages| messages.first())
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            raw_metadata: ack,
        })
    }

    fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = self.url(path);
        debug!("ibkr GET {}", url);
        let response = self
            .client
            .get(url)
            .send()
            .map_err(|err| TradeBotError::broker("ibkr", err.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(TradeBotError::broker(
                "ibkr",
                format!("GET {path} failed with status {status}"),
            ));
        }
        response
            .json::<T>()
            .map_err(|err| TradeBotError::broker("ibkr", err.to_string()))
    }

    fn post_json(&self, path: &str, body: &Value) -> Result<Value> {
        let url = self.url(path);
        debug!("ibkr POST {}", url);
        let response = self
            .client
            .post(url)
            .json(body)
            .send()
            .map_err(|err| TradeBotError::broker("ibkr", err.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(TradeBotError::broker(
                "ibkr",
                format!("POST {path} failed with status {status}"),
            ));
        }
        response
            .json::<Value>()
            .map_err(|err| TradeBotError::broker("ibkr", err.to_string()))
    }

    fn delete_json(&self, path: &str) -> Result<Value> {
        let url = self.url(path);
        debug!("ibkr DELETE {}", url);
        let response = self
            .client
            .delete(url)
            .send()
            .map_err(|err| TradeBotError::broker("ibkr", err.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(TradeBotError::broker(
                "ibkr",
                format!("DELETE {path} failed with status {status}"),
            ));
        }
        response
            .json::<Value>()
            .map_err(|err| TradeBotError::broker("ibkr", err.to_string()))
    }

    fn url(&self, path: &str) -> String {
        let base = self.settings.base_url.trim_end_matches('/');
        format!("{base}/{path}")
    }
}

fn parse_settings(config: &BrokerConfig) -> Result<IbkrSettings> {
    let value = toml::Value::Table(config.settings.clone().into_iter().collect());
    value
        .try_into()
        .map_err(|err| TradeBotError::Config(format!("invalid ibkr settings: {err}")))
}

fn parse_ibkr_number(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::String(raw)) => raw.replace(',', "").parse::<f64>().ok(),
        Some(Value::Number(num)) => num.as_f64(),
        _ => None,
    }
}

fn parse_conid_value(conid: Option<&str>) -> Value {
    conid
        .and_then(|raw| raw.parse::<i64>().ok())
        .map_or(Value::Null, |parsed| json!(parsed))
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(raw) => Some(raw.clone()),
        Value::Number(num) => Some(num.to_string()),
        _ => None,
    }
}

fn matches_cancel_filter(row: &Value, request: &CancelRequest) -> bool {
    let symbol_match = request.all_open
        || request.instruments.iter().any(|instrument| {
            row.get("ticker")
                .and_then(Value::as_str)
                .map(|ticker| ticker.eq_ignore_ascii_case(&instrument.ticker))
                .unwrap_or(false)
                || row
                    .get("conid")
                    .and_then(value_to_string)
                    .zip(instrument.conid.clone())
                    .map(|(left, right)| left == right)
                    .unwrap_or(false)
        });

    let side_match = request.side.map_or(true, |side| {
        row.get("side")
            .and_then(Value::as_str)
            .map(|raw| raw.eq_ignore_ascii_case(side.as_uppercase()))
            .unwrap_or(false)
    });

    let tag_match = request.client_tag.as_ref().map_or(true, |client_tag| {
        row.get("order_ref")
            .and_then(Value::as_str)
            .map(|raw| raw == client_tag)
            .unwrap_or(false)
    });

    symbol_match && side_match && tag_match
}
