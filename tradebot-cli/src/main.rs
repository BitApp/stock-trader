mod notify;
mod watch;

use std::path::PathBuf;

use broker_ibkr::IbkrBrokerFactory;
use broker_longbridge::LongbridgeBrokerFactory;
use clap::{Parser, Subcommand, ValueEnum};
use tracing_subscriber::{EnvFilter, fmt};
use trading_core::{
    AppConfig, BrokerRegistry, NotificationEvent, Result, TradeBotError, TradingEngine,
};

#[derive(Debug, Parser)]
#[command(name = "tradebot")]
#[command(about = "Unified multi-broker stock trading CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Validate {
        #[arg(long, conflicts_with = "config_dir")]
        config: Option<PathBuf>,
        #[arg(long, conflicts_with = "config")]
        config_dir: Option<PathBuf>,
    },
    Run {
        #[arg(long, conflicts_with = "config_dir")]
        config: Option<PathBuf>,
        #[arg(long, conflicts_with = "config")]
        config_dir: Option<PathBuf>,
        #[arg(long)]
        task: String,
    },
    OrderStatus {
        #[arg(long, conflicts_with = "config_dir")]
        config: Option<PathBuf>,
        #[arg(long, conflicts_with = "config")]
        config_dir: Option<PathBuf>,
        #[arg(long)]
        broker: String,
        #[arg(long)]
        broker_order_id: String,
    },
    OpenOrders {
        #[arg(long, conflicts_with = "config_dir")]
        config: Option<PathBuf>,
        #[arg(long, conflicts_with = "config")]
        config_dir: Option<PathBuf>,
        #[arg(long)]
        broker: String,
    },
    PreviewNotify {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        task: String,
        #[arg(long, value_enum)]
        event: PreviewEvent,
        #[arg(long)]
        error: Option<String>,
    },
    Watch {
        #[arg(long, conflicts_with = "config_dir")]
        config: Option<PathBuf>,
        #[arg(long, conflicts_with = "config")]
        config_dir: Option<PathBuf>,
        #[arg(long, default_value_t = 5)]
        poll_seconds: u64,
    },
}

#[derive(Debug, Clone, ValueEnum)]
enum PreviewEvent {
    Success,
    Failure,
    Filled,
    PartialFilled,
}

impl From<PreviewEvent> for NotificationEvent {
    fn from(value: PreviewEvent) -> Self {
        match value {
            PreviewEvent::Success => NotificationEvent::Success,
            PreviewEvent::Failure => NotificationEvent::Failure,
            PreviewEvent::Filled => NotificationEvent::Filled,
            PreviewEvent::PartialFilled => NotificationEvent::PartialFilled,
        }
    }
}

fn main() {
    init_rustls();
    init_logging();
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Validate { config, config_dir } => {
            let loaded = load_app_config(config, config_dir)?;
            let engine = build_engine(loaded.clone());
            for task in &loaded.tasks {
                let report = engine.validate_task(&task.name)?;
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
        }
        Command::Run {
            config,
            config_dir,
            task,
        } => {
            let loaded = load_app_config(config, config_dir)?;
            let task_config = loaded.task(&task)?.clone();
            let engine = build_engine(loaded.clone());
            match engine.run_task(&task) {
                Ok(result) => {
                    println!("{}", serde_json::to_string_pretty(&result)?);
                    notify::notify_task_success(&loaded, &task_config, &result);
                }
                Err(err) => {
                    notify::notify_task_failure(&loaded, &task_config, &err.to_string());
                    return Err(err);
                }
            }
        }
        Command::OrderStatus {
            config,
            config_dir,
            broker,
            broker_order_id,
        } => {
            let loaded = load_app_config(config, config_dir)?;
            let engine = build_engine(loaded);
            let report = engine.query_order_status(&broker, &broker_order_id)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::OpenOrders {
            config,
            config_dir,
            broker,
        } => {
            let loaded = load_app_config(config, config_dir)?;
            let engine = build_engine(loaded);
            let report = engine.list_open_orders(&broker)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::PreviewNotify {
            config,
            task,
            event,
            error,
        } => {
            let loaded = AppConfig::load(&config)?;
            let task_config = loaded.task(&task)?.clone();
            let recipients = task_config
                .notify
                .as_ref()
                .and_then(|notify| notify.email.as_ref())
                .map(|email| email.to.join(", "))
                .unwrap_or_default();
            let preview = notify::send_preview_notification(
                &loaded,
                &task_config,
                event.into(),
                error.as_deref(),
            )
            .map_err(TradeBotError::Config)?;
            if !recipients.is_empty() {
                println!("Sent to: {recipients}");
            }
            println!("Subject: {}\n", preview.subject);
            println!("{}", preview.body);
        }
        Command::Watch {
            config,
            config_dir,
            poll_seconds,
        } => watch::watch(config, config_dir, poll_seconds)?,
    }
    Ok(())
}

fn load_app_config(config: Option<PathBuf>, config_dir: Option<PathBuf>) -> Result<AppConfig> {
    watch::load_app_config(config, config_dir)
}

fn build_engine(config: AppConfig) -> TradingEngine {
    let mut registry = BrokerRegistry::new();
    registry.register(LongbridgeBrokerFactory);
    registry.register(IbkrBrokerFactory);
    TradingEngine::new(config, registry)
}

fn init_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt().with_env_filter(filter).with_target(false).try_init();
}

fn init_rustls() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
