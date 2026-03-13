mod notify;
mod watch;

use std::path::PathBuf;

use broker_ibkr::IbkrBrokerFactory;
use broker_longbridge::LongbridgeBrokerFactory;
use clap::{Parser, Subcommand};
use tracing_subscriber::{EnvFilter, fmt};
use trading_core::{AppConfig, BrokerRegistry, Result, TradingEngine};

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
        #[arg(long)]
        config: PathBuf,
    },
    Run {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        task: String,
    },
    Watch {
        #[arg(long)]
        config: PathBuf,
        #[arg(long, default_value_t = 5)]
        poll_seconds: u64,
    },
}

fn main() {
    init_logging();
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Validate { config } => {
            let loaded = AppConfig::load(&config)?;
            let engine = build_engine(loaded.clone());
            for task in &loaded.tasks {
                let report = engine.validate_task(&task.name)?;
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
        }
        Command::Run { config, task } => {
            let loaded = AppConfig::load(&config)?;
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
        Command::Watch {
            config,
            poll_seconds,
        } => watch::watch(config, poll_seconds)?,
    }
    Ok(())
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
