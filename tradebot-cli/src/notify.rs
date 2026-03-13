use std::collections::BTreeMap;
use std::env;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_sesv2::types::{Body, Content, Destination, EmailContent, Message};
use chrono::{Offset, Utc};
use tokio::runtime::Builder;
use tracing::warn;
use trading_core::{
    AppConfig, EmailNotificationConfig, ExecutionResult, NotificationEvent, TaskAction, TaskConfig,
};

const EMAIL_REGION_ENV: &str = "TRADEBOT_EMAIL_REGION";
const EMAIL_SENDER_ENV: &str = "TRADEBOT_EMAIL_SENDER";

pub fn notify_task_success(config: &AppConfig, task: &TaskConfig, result: &ExecutionResult) {
    dispatch_notification(
        task,
        NotificationEvent::Success,
        &format_subject(config, task, "completed"),
        &format_success_body(config, result),
    );
    if task_result_is_filled(result) {
        dispatch_notification(
            task,
            NotificationEvent::Filled,
            &format_subject(config, task, "filled"),
            &format_filled_body(config, result),
        );
    } else if task_result_is_partially_filled(result) {
        dispatch_notification(
            task,
            NotificationEvent::PartialFilled,
            &format_subject(config, task, "partial_filled"),
            &format_partial_filled_body(config, result),
        );
    }
}

pub fn notify_task_failure(config: &AppConfig, task: &TaskConfig, error: &str) {
    dispatch_notification(
        task,
        NotificationEvent::Failure,
        &format_subject(config, task, "failed"),
        &format_failure_body(config, task, error),
    );
}

fn dispatch_notification(task: &TaskConfig, event: NotificationEvent, subject: &str, body: &str) {
    let Some(email_config) = task
        .notify
        .as_ref()
        .and_then(|notify| notify.email.as_ref())
    else {
        return;
    };
    if !email_config.on.contains(&event) {
        return;
    }

    if let Err(err) = send_email_blocking(email_config, subject, body) {
        warn!(
            task = %task.name,
            error = %err,
            "failed to send task email notification"
        );
    }
}

fn send_email_blocking(
    email: &EmailNotificationConfig,
    subject: &str,
    body: &str,
) -> Result<(), String> {
    let region = required_env(EMAIL_REGION_ENV)?;
    let sender = required_env(EMAIL_SENDER_ENV)?;

    let runtime = Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| format!("build tokio runtime: {err}"))?;

    runtime.block_on(async {
        let shared_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region))
            .load()
            .await;
        let client = aws_sdk_sesv2::Client::new(&shared_config);

        let subject_content = Content::builder()
            .data(subject)
            .charset("UTF-8")
            .build()
            .map_err(|err| format!("build email subject: {err}"))?;
        let body_content = Content::builder()
            .data(body)
            .charset("UTF-8")
            .build()
            .map_err(|err| format!("build email body: {err}"))?;
        let message = Message::builder()
            .subject(subject_content)
            .body(Body::builder().text(body_content).build())
            .build();
        let destination = Destination::builder()
            .set_to_addresses(Some(email.to.clone()))
            .build();
        let content = EmailContent::builder().simple(message).build();

        client
            .send_email()
            .from_email_address(&sender)
            .destination(destination)
            .content(content)
            .send()
            .await
            .map_err(|err| format!("ses send_email: {err}"))?;

        Ok(())
    })
}

fn required_env(name: &str) -> Result<String, String> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value),
        Ok(_) => Err(format!("environment variable `{name}` is set but empty")),
        Err(_) => Err(format!("missing required environment variable `{name}`")),
    }
}

fn format_subject(config: &AppConfig, task: &TaskConfig, status: &str) -> String {
    let prefix = config
        .defaults
        .email
        .as_ref()
        .and_then(|email| email.subject_prefix.as_deref())
        .map(|prefix| format!("{prefix} "))
        .unwrap_or_default();
    format!("{prefix}task {} {}", task.name, status)
}

fn format_success_body(config: &AppConfig, result: &ExecutionResult) -> String {
    let timestamp = timestamp_in_config_timezone(config);
    let payload = serde_json::to_string_pretty(result)
        .unwrap_or_else(|_| "{\"error\":\"failed to serialize execution result\"}".into());

    format!(
        "Task finished successfully.\n\nTime: {timestamp}\nTask: {}\nBroker: {} ({})\nAction: {:?}\nOrders: {}\nCancellations: {}\nWarnings: {}\n\nResult:\n{payload}",
        result.task_name,
        result.broker_name,
        result.broker_kind,
        result.action,
        result.orders.len(),
        result.cancellations.len(),
        result.warnings.len(),
    )
}

fn format_failure_body(config: &AppConfig, task: &TaskConfig, error: &str) -> String {
    let timestamp = timestamp_in_config_timezone(config);

    format!(
        "Task finished with an error.\n\nTime: {timestamp}\nTask: {}\nBroker: {}\nAction: {:?}\nError: {error}",
        task.name, task.broker, task.action
    )
}

fn format_filled_body(config: &AppConfig, result: &ExecutionResult) -> String {
    let timestamp = timestamp_in_config_timezone(config);
    let payload = serde_json::to_string_pretty(result)
        .unwrap_or_else(|_| "{\"error\":\"failed to serialize execution result\"}".into());

    format!(
        "Task finished with all tracked orders filled.\n\nTime: {timestamp}\nTask: {}\nBroker: {} ({})\nAction: {:?}\nOrders: {}\nCancellations: {}\nWarnings: {}\n\nResult:\n{payload}",
        result.task_name,
        result.broker_name,
        result.broker_kind,
        result.action,
        result.orders.len(),
        result.cancellations.len(),
        result.warnings.len(),
    )
}

fn format_partial_filled_body(config: &AppConfig, result: &ExecutionResult) -> String {
    let timestamp = timestamp_in_config_timezone(config);
    let payload = serde_json::to_string_pretty(result)
        .unwrap_or_else(|_| "{\"error\":\"failed to serialize execution result\"}".into());

    format!(
        "Task finished with partial fills.\n\nTime: {timestamp}\nTask: {}\nBroker: {} ({})\nAction: {:?}\nOrders: {}\nCancellations: {}\nWarnings: {}\n\nResult:\n{payload}",
        result.task_name,
        result.broker_name,
        result.broker_kind,
        result.action,
        result.orders.len(),
        result.cancellations.len(),
        result.warnings.len(),
    )
}

fn task_result_is_filled(result: &ExecutionResult) -> bool {
    if result.action != TaskAction::Place || result.orders.is_empty() {
        return false;
    }

    latest_tracked_orders(result)
        .into_iter()
        .all(order_is_filled)
}

fn task_result_is_partially_filled(result: &ExecutionResult) -> bool {
    if result.action != TaskAction::Place
        || result.orders.is_empty()
        || task_result_is_filled(result)
    {
        return false;
    }

    latest_tracked_orders(result)
        .into_iter()
        .any(order_has_any_fill)
}

fn latest_tracked_orders(result: &ExecutionResult) -> Vec<&trading_core::OrderResult> {
    let mut latest_by_symbol = BTreeMap::new();
    let mut fallback_orders = Vec::new();

    for order in &result.orders {
        if let Some(symbol) = order
            .raw_metadata
            .get("symbol")
            .and_then(serde_json::Value::as_str)
        {
            latest_by_symbol.insert(symbol.to_string(), order);
        } else {
            fallback_orders.push(order);
        }
    }

    if latest_by_symbol.is_empty() {
        result.orders.iter().collect()
    } else {
        latest_by_symbol
            .into_values()
            .chain(fallback_orders)
            .collect()
    }
}

fn order_is_filled(order: &trading_core::OrderResult) -> bool {
    order.status.eq_ignore_ascii_case("filled")
}

fn order_has_any_fill(order: &trading_core::OrderResult) -> bool {
    order.filled_qty.unwrap_or(0.0) > 0.0
        || order.status.eq_ignore_ascii_case("partially_filled")
        || order.status.eq_ignore_ascii_case("partial_filled")
}

fn timestamp_in_config_timezone(config: &AppConfig) -> String {
    let timezone = config
        .defaults
        .parse_timezone()
        .ok()
        .unwrap_or(chrono_tz::UTC);
    let now = Utc::now().with_timezone(&timezone);
    format!("{} {}", now.format("%Y-%m-%d %H:%M:%S"), now.offset().fix())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use trading_core::{ExecutionResult, OrderResult, TaskAction};

    use super::{task_result_is_filled, task_result_is_partially_filled};

    fn sample_order(status: &str, symbol: &str, filled_qty: Option<f64>) -> OrderResult {
        OrderResult {
            broker_order_id: format!("broker-{symbol}-{status}"),
            client_order_id: format!("client-{symbol}-{status}"),
            status: status.into(),
            filled_qty,
            avg_price: None,
            message: None,
            raw_metadata: json!({ "symbol": symbol }),
        }
    }

    fn sample_result(orders: Vec<OrderResult>) -> ExecutionResult {
        ExecutionResult {
            task_name: "task".into(),
            broker_name: "broker".into(),
            broker_kind: "kind".into(),
            action: TaskAction::Place,
            orders,
            cancellations: Vec::new(),
            warnings: Vec::new(),
        }
    }

    #[test]
    fn filled_notification_requires_latest_order_per_symbol_to_be_filled() {
        let result = sample_result(vec![
            sample_order("cancelled_for_retry", "SPY", Some(0.0)),
            sample_order("filled", "SPY", Some(10.0)),
            sample_order("filled", "QQQ", Some(5.0)),
        ]);

        assert!(task_result_is_filled(&result));
    }

    #[test]
    fn filled_notification_skips_submitted_orders() {
        let result = sample_result(vec![sample_order("submitted", "SPY", None)]);

        assert!(!task_result_is_filled(&result));
    }

    #[test]
    fn partial_filled_notification_requires_fill_but_not_all_filled() {
        let result = sample_result(vec![
            sample_order("partially_filled", "SPY", Some(3.0)),
            sample_order("submitted", "QQQ", None),
        ]);

        assert!(task_result_is_partially_filled(&result));
        assert!(!task_result_is_filled(&result));
    }

    #[test]
    fn partial_filled_notification_skips_fully_filled_tasks() {
        let result = sample_result(vec![sample_order("filled", "SPY", Some(10.0))]);

        assert!(!task_result_is_partially_filled(&result));
    }
}
