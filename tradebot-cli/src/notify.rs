use std::env;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_sesv2::types::{Body, Content, Destination, EmailContent, Message};
use chrono::{Offset, Utc};
use tokio::runtime::Builder;
use tracing::warn;
use trading_core::{
    AppConfig, EmailNotificationConfig, ExecutionResult, NotificationEvent, TaskConfig,
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

fn timestamp_in_config_timezone(config: &AppConfig) -> String {
    let timezone = config
        .defaults
        .parse_timezone()
        .ok()
        .unwrap_or(chrono_tz::UTC);
    let now = Utc::now().with_timezone(&timezone);
    format!("{} {}", now.format("%Y-%m-%d %H:%M:%S"), now.offset().fix())
}
