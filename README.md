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

Or watch a directory of task files:

```bash
cargo run -p tradebot-cli -- watch --config-dir config/tasks
```

Scheduled tasks are declared inline on each task with a local-time `schedule` block:

```toml
[defaults]
timezone = "Asia/Shanghai"

[[tasks]]
schedule = { time = "09:30", weekdays = ["mon", "tue", "wed", "thu", "fri"], overdue_policy = "run" }
```

`defaults.timezone` determines how scheduled task times are interpreted. `schedule.overdue_policy` defaults to `run`, which backfills a task once the watcher notices the scheduled time has already passed. Set `overdue_policy = "skip"` to suppress those catch-up runs and only fire when the watcher actually crosses the scheduled time while active. Edit the config file while `watch` is running and the process will apply the next valid version without a restart. Config reloads are triggered by file-system events; the watch loop timer is only used to wake up and check whether any scheduled task is now due. With `--config-dir`, the watcher loads every `*.toml` in the directory, merges them, and hot-reloads file additions, removals, and edits. Shared files may contain only common `defaults`, `brokers`, `watch`, or `task_templates` definitions.

Broker connectivity now comes from environment variables rather than inline broker settings:

```bash
export LONGPORT_APP_KEY=...
export LONGPORT_APP_SECRET=...
export LONGPORT_ACCESS_TOKEN=...

export IBKR_BASE_URL=https://127.0.0.1:5000/v1/api
export IBKR_ACCOUNT_ID=DU123456
export IBKR_ALLOW_INSECURE_TLS=true
export IBKR_AUTO_CONFIRM_REPLIES=true
```

If you need multiple accounts for the same broker type in one config, keep credentials in environment variables and add an `env_prefix` per broker instance:

```toml
[brokers.longbridge_main]
kind = "longbridge"
env_prefix = "LONGPORT_MAIN"

[brokers.longbridge_alt]
kind = "longbridge"
env_prefix = "LONGPORT_ALT"

[brokers.ibkr_main]
kind = "ibkr"
env_prefix = "IBKR_MAIN"
```

These broker instances will read:

```bash
export LONGPORT_MAIN_APP_KEY=...
export LONGPORT_MAIN_APP_SECRET=...
export LONGPORT_MAIN_ACCESS_TOKEN=...

export LONGPORT_ALT_APP_KEY=...
export LONGPORT_ALT_APP_SECRET=...
export LONGPORT_ALT_ACCESS_TOKEN=...

export IBKR_MAIN_BASE_URL=https://127.0.0.1:5000/v1/api
export IBKR_MAIN_ACCOUNT_ID=DU123456
export IBKR_MAIN_ALLOW_INSECURE_TLS=true
export IBKR_MAIN_AUTO_CONFIRM_REPLIES=true
```

When `env_prefix` is omitted, the runtime keeps using the legacy variable names such as `LONGPORT_APP_KEY` and `IBKR_ACCOUNT_ID`.

Repeated task fields can be extracted into reusable templates:

```toml
[task_templates.ibkr_us_place]
broker = "ibkr"
side = "buy"
pricing = { kind = "counterparty" }
shared_budget = { amount = 10000.0 }
client_tag = "pm2-us-rebalance"
notify = { email = { to = ["ops@example.com"], on = ["success", "failure"] } }

[[tasks]]
name = "ibkr_us_rebalance"
template = "ibkr_us_place"

[[tasks.symbols]]
ticker = "AAPL"
market = "us"
conid = "265598"
weight = 1.0
```

Tasks override template fields when both are present. Arrays and nested tables are replaced wholesale, so a task can swap out `symbols`, `notify`, `schedule`, or `execution` without deep-merge rules.

Task completion emails can be enabled with a task-level recipient list:

```toml
[defaults.email]
subject_prefix = "[stock-trader]"

[[tasks]]
note = "Morning rebalance for the core US basket."
notify = { email = { to = ["ops@example.com"], on = ["success", "failure"] } }
```

`on` defaults to `["success"]`. When enabled, both `run` and scheduled `watch` executions send multipart text/HTML emails after the task ends. If a task sets `note`, that text is included in the notification body.

You can also render and send a notification template manually with:

```bash
cargo run -p tradebot-cli -- preview-notify --config config/example.toml --task ibkr_us_rebalance --event success
```

`preview-notify` sends to the task's configured `notify.email.to` recipients and prints the plain-text version of the subject/body that was sent.

Supported notification events are:
- `success`: task returned successfully
- `failure`: task returned an error
- `filled`: all tracked orders in the result ended in `filled`
- `partial_filled`: at least one tracked order had fill quantity, but not all tracked orders ended in `filled`

The watcher can also send email when the effective task list is loaded or changes after a config reload:

```toml
[watch.notify.email]
to = ["ops@example.com"]
on = ["task_list_loaded", "task_list_changed"]
```

`task_list_loaded` fires after the initial successful load, and `task_list_changed` fires after a successful reload that changes any expanded task config.

The SES transport reads these environment variables at runtime:

```bash
export TRADEBOT_EMAIL_REGION=ap-southeast-1
export TRADEBOT_EMAIL_SENDER=bot@example.com
```

The process also needs valid AWS credentials with `ses:SendEmail` permission. `TRADEBOT_EMAIL_SENDER` must be a verified SES identity in the same region as `TRADEBOT_EMAIL_REGION`. If your SES account is still in sandbox mode, every recipient in `notify.email.to` must also be verified.

Managed order policies can be attached to `place` tasks. For example, this will place at the current counterparty price, wait 5 minutes, then cancel and resubmit if still unfilled:

```toml
pricing = { kind = "counterparty" }
execution = { kind = "cancel_replace", timeout_seconds = 300, poll_seconds = 5, max_attempts = 3 }
```

If you want the default submit-once behavior but prefer explicit config, use:

```toml
execution = { kind = "one_shot" }
```

If you want fill tracking without any retry logic, use `track` instead:

```toml
pricing = { kind = "market" }
execution = { kind = "track", timeout_seconds = 300, poll_seconds = 30 }
```

`track` submits once, polls broker order status until the order becomes terminal or the timeout elapses, and never cancels or replaces the order. This is the execution mode to use when you want `filled` or `partial_filled` notifications for a one-shot order.

Extended-hours trading can be enabled per `place` task for brokers that support it:

```toml
session = { extended_hours = true }
```

This sets IBKR `outsideRTH = true` and Longbridge `outside_rth = ANY_TIME`. It is intended for US pre-market and post-market eligibility.

## Notes

- `IBKR` requires a running `Client Portal Gateway` session.
- `Longbridge` now supports live position lookup, quote lookup, order submission, order-detail polling, and cancel-by-id for managed execution loops.
- `IBKR` now supports managed cancel-replace execution via order polling from `iserver/account/orders` and cancel-by-id.
- The public API is centered around `trading_core::TradingEngine`.
