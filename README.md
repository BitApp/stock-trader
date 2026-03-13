# stock-trader

Rust workspace for a unified multi-broker stock trading engine.

Current scope:

- Unified `trading-core` API for task validation and execution
- `IBKR` adapter using Client Portal Gateway Web API
- `Longbridge` adapter scaffold behind the same broker interface
- Thin CLI for `run` and `validate`

## Workspace

- `trading-core`: config parsing, task validation, broker traits, allocation engine
- `broker-ibkr`: Interactive Brokers Web API adapter
- `broker-longbridge`: Longbridge adapter scaffold
- `tradebot-cli`: command line entry point

## Quick start

1. Export broker credentials into environment variables.
2. Copy `config/example.toml` and adjust broker/task definitions.
3. Validate:

```bash
cargo run -p tradebot-cli -- validate --config config/example.toml
```

4. Execute one task:

```bash
cargo run -p tradebot-cli -- run --config config/example.toml --task ibkr_us_rebalance
```

## Notes

- `IBKR` requires a running `Client Portal Gateway` session.
- `Longbridge` is already wired into the unified broker registry, but live trading calls are still intentionally stubbed behind explicit runtime errors.
- The public API is centered around `trading_core::TradingEngine`.

