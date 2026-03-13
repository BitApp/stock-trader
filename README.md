# stock-trader

Rust workspace for a unified multi-broker stock trading engine.

Current scope:

- Unified `trading-core` API for task validation and execution
- `IBKR` adapter using Client Portal Gateway Web API
- `Longbridge` adapter scaffold behind the same broker interface
- Thin CLI for `run`, `validate`, and long-running scheduled `watch`

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

5. Run the hot-reloading scheduler:

```bash
cargo run -p tradebot-cli -- watch --config config/example.toml
```

Scheduled tasks are declared inline on each task with a local-time `schedule` block:

```toml
[defaults]
timezone = "Asia/Shanghai"

[[tasks]]
schedule = { time = "09:30", weekdays = ["mon", "tue", "wed", "thu", "fri"] }
```

`defaults.timezone` determines how scheduled task times are interpreted. Edit the config file while `watch` is running and the process will apply the next valid version without a restart.

Task completion emails can be enabled with a task-level recipient list:

```toml
[defaults.email]
subject_prefix = "[stock-trader]"

[[tasks]]
notify = { email = { to = ["ops@example.com"], on = ["success", "failure"] } }
```

`on` defaults to `["success"]`. When enabled, both `run` and scheduled `watch` executions send a plain-text email after the task ends.

The SES transport reads these environment variables at runtime:

```bash
export TRADEBOT_EMAIL_REGION=ap-southeast-1
export TRADEBOT_EMAIL_SENDER=bot@example.com
```

Managed order policies can be attached to `place` tasks. For example, this will place at the current counterparty price, wait 5 minutes, then cancel and resubmit if still unfilled:

```toml
pricing = { kind = "counterparty" }
execution = { kind = "cancel_replace", timeout_seconds = 300, poll_seconds = 5, max_attempts = 3 }
```

## Notes

- `IBKR` requires a running `Client Portal Gateway` session.
- `Longbridge` now supports live position lookup, quote lookup, order submission, order-detail polling, and cancel-by-id for managed execution loops.
- `IBKR` now supports managed cancel-replace execution via order polling from `iserver/account/orders` and cancel-by-id.
- The public API is centered around `trading_core::TradingEngine`.
