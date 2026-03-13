use std::io;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, TradeBotError>;

#[derive(Debug, Error)]
pub enum TradeBotError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("toml parse error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("config error: {0}")]
    Config(String),

    #[error("validation error: {0}")]
    Validation(String),

    #[error("broker `{broker}` error: {message}")]
    Broker { broker: String, message: String },

    #[error("not found: {0}")]
    NotFound(String),

    #[error("unsupported: {0}")]
    Unsupported(String),
}

impl TradeBotError {
    pub fn broker(broker: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Broker {
            broker: broker.into(),
            message: message.into(),
        }
    }
}
