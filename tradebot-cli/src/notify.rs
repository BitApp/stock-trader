use std::collections::BTreeMap;
use std::env;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_sesv2::error::{DisplayErrorContext, ProvideErrorMetadata};
use aws_sdk_sesv2::operation::send_email::SendEmailError;
use aws_sdk_sesv2::types::{Body, Content, Destination, EmailContent, Message};
use chrono::{Offset, Utc};
use serde_json::json;
use tokio::runtime::Builder;
use tracing::warn;
use trading_core::{
    AppConfig, ExecutionResult, NotificationEvent, OrderResult, TaskAction, TaskConfig,
    WatchNotificationEvent,
};

const EMAIL_REGION_ENV: &str = "TRADEBOT_EMAIL_REGION";
const EMAIL_SENDER_ENV: &str = "TRADEBOT_EMAIL_SENDER";

pub struct NotificationPreview {
    pub subject: String,
    pub body: String,
    pub html_body: String,
}

#[derive(Debug)]
struct TaskListCounts {
    enabled: usize,
    total: usize,
}

#[derive(Debug)]
struct UpdatedTaskEntry {
    name: String,
    before: String,
    after: String,
}

#[derive(Debug)]
struct TaskListChangeDetails {
    before: TaskListCounts,
    after: TaskListCounts,
    added: Vec<String>,
    removed: Vec<String>,
    updated: Vec<UpdatedTaskEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskDisplayState {
    Active,
    Disabled,
    Manual,
}

pub fn notify_task_success(config: &AppConfig, task: &TaskConfig, result: &ExecutionResult) {
    let subject = format_subject(config, task, "completed");
    let body = format_success_body(config, task, result);
    let html_body = format_success_html(config, task, result);
    dispatch_notification(
        task,
        NotificationEvent::Success,
        &subject,
        &body,
        &html_body,
    );
    if task_result_is_filled(result) {
        let subject = format_subject(config, task, "filled");
        let body = format_filled_body(config, task, result);
        let html_body = format_filled_html(config, task, result);
        dispatch_notification(task, NotificationEvent::Filled, &subject, &body, &html_body);
    } else if task_result_is_partially_filled(result) {
        let subject = format_subject(config, task, "partial_filled");
        let body = format_partial_filled_body(config, task, result);
        let html_body = format_partial_filled_html(config, task, result);
        dispatch_notification(
            task,
            NotificationEvent::PartialFilled,
            &subject,
            &body,
            &html_body,
        );
    }
}

pub fn notify_task_failure(config: &AppConfig, task: &TaskConfig, error: &str) {
    let subject = format_subject(config, task, "failed");
    let body = format_failure_body(config, task, error);
    let html_body = format_failure_html(config, task, error);
    dispatch_notification(
        task,
        NotificationEvent::Failure,
        &subject,
        &body,
        &html_body,
    );
}

pub fn notify_task_list_loaded(config: &AppConfig, source: &str) {
    let subject = format_watch_subject(config, "task list loaded");
    let body = format_task_list_loaded_body(config, source);
    let html_body = format_task_list_loaded_html(config, source);
    dispatch_watch_notification(
        config,
        WatchNotificationEvent::TaskListLoaded,
        &subject,
        &body,
        &html_body,
    );
}

pub fn notify_task_list_changed(previous: &AppConfig, current: &AppConfig, source: &str) {
    let Some(details) = task_list_change_details(previous, current) else {
        return;
    };

    let subject = format_watch_subject(current, "task list changed");
    let body = format_task_list_changed_body(previous, current, source, &details);
    let html_body = format_task_list_changed_html(previous, current, source, &details);
    dispatch_watch_notification(
        current,
        WatchNotificationEvent::TaskListChanged,
        &subject,
        &body,
        &html_body,
    );
}

pub fn preview_notification(
    config: &AppConfig,
    task: &TaskConfig,
    event: NotificationEvent,
    failure_error: Option<&str>,
) -> Result<NotificationPreview, String> {
    match event {
        NotificationEvent::Success => {
            let result = sample_result_for_event(task, event)?;
            Ok(NotificationPreview {
                subject: format_subject(config, task, "completed"),
                body: format_success_body(config, task, &result),
                html_body: format_success_html(config, task, &result),
            })
        }
        NotificationEvent::Failure => Ok(NotificationPreview {
            subject: format_subject(config, task, "failed"),
            body: format_failure_body(
                config,
                task,
                failure_error.unwrap_or("preview failure message"),
            ),
            html_body: format_failure_html(
                config,
                task,
                failure_error.unwrap_or("preview failure message"),
            ),
        }),
        NotificationEvent::Filled => {
            let result = sample_result_for_event(task, event)?;
            Ok(NotificationPreview {
                subject: format_subject(config, task, "filled"),
                body: format_filled_body(config, task, &result),
                html_body: format_filled_html(config, task, &result),
            })
        }
        NotificationEvent::PartialFilled => {
            let result = sample_result_for_event(task, event)?;
            Ok(NotificationPreview {
                subject: format_subject(config, task, "partial_filled"),
                body: format_partial_filled_body(config, task, &result),
                html_body: format_partial_filled_html(config, task, &result),
            })
        }
    }
}

pub fn send_preview_notification(
    config: &AppConfig,
    task: &TaskConfig,
    event: NotificationEvent,
    failure_error: Option<&str>,
) -> Result<NotificationPreview, String> {
    let email = task
        .notify
        .as_ref()
        .and_then(|notify| notify.email.as_ref())
        .ok_or_else(|| {
            format!(
                "task `{}` has no notify.email recipients configured",
                task.name
            )
        })?;

    let preview = preview_notification(config, task, event, failure_error)?;
    send_email_blocking(
        &email.to,
        &preview.subject,
        &preview.body,
        &preview.html_body,
    )?;
    Ok(preview)
}

fn dispatch_notification(
    task: &TaskConfig,
    event: NotificationEvent,
    subject: &str,
    body: &str,
    html_body: &str,
) {
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

    if let Err(err) = send_email_blocking(&email_config.to, subject, body, html_body) {
        warn!(
            task = %task.name,
            event = ?event,
            recipients = %email_config.to.join(","),
            error = %err,
            "failed to send task email notification"
        );
    }
}

fn dispatch_watch_notification(
    config: &AppConfig,
    event: WatchNotificationEvent,
    subject: &str,
    body: &str,
    html_body: &str,
) {
    let Some(email_config) = config
        .watch
        .notify
        .as_ref()
        .and_then(|notify| notify.email.as_ref())
    else {
        return;
    };
    if !email_config.on.contains(&event) {
        return;
    }

    if let Err(err) = send_email_blocking(&email_config.to, subject, body, html_body) {
        warn!(
            event = ?event,
            recipients = %email_config.to.join(","),
            error = %err,
            "failed to send watch email notification"
        );
    }
}

fn send_email_blocking(
    recipients: &[String],
    subject: &str,
    body: &str,
    html_body: &str,
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
        let html_body_content = Content::builder()
            .data(html_body)
            .charset("UTF-8")
            .build()
            .map_err(|err| format!("build html email body: {err}"))?;
        let message = Message::builder()
            .subject(subject_content)
            .body(
                Body::builder()
                    .text(body_content)
                    .html(html_body_content)
                    .build(),
            )
            .build();
        let destination = Destination::builder()
            .set_to_addresses(Some(recipients.to_vec()))
            .build();
        let content = EmailContent::builder().simple(message).build();

        client
            .send_email()
            .from_email_address(&sender)
            .destination(destination)
            .content(content)
            .send()
            .await
            .map_err(format_send_email_error)?;

        Ok(())
    })
}

fn format_send_email_error(err: aws_sdk_sesv2::error::SdkError<SendEmailError>) -> String {
    let context = DisplayErrorContext(&err).to_string();
    let code = err.code().unwrap_or("unknown");
    let message = err.message().unwrap_or("unknown");

    format!("ses send_email: code={code} message={message} context={context}")
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
    format!("{prefix}{} {}", task.name, status)
}

fn format_watch_subject(config: &AppConfig, status: &str) -> String {
    let prefix = config
        .defaults
        .email
        .as_ref()
        .and_then(|email| email.subject_prefix.as_deref())
        .map(|prefix| format!("{prefix} "))
        .unwrap_or_default();
    format!("{prefix}{status}")
}

fn format_success_body(config: &AppConfig, task: &TaskConfig, result: &ExecutionResult) -> String {
    format_result_text(
        config,
        task,
        result,
        "completed",
        "Task completed successfully.",
    )
}

fn format_failure_body(config: &AppConfig, task: &TaskConfig, error: &str) -> String {
    let timestamp = timestamp_in_config_timezone(config);
    let note_section = format_task_note_section(task);

    format!(
        "Task failed.\n\nSummary\nTime: {timestamp}\nTask: {}\nBroker: {}\nAction: {}{note_section}\n\nError\n{error}",
        task.name,
        task.broker,
        format_action(task.action)
    )
}

fn format_filled_body(config: &AppConfig, task: &TaskConfig, result: &ExecutionResult) -> String {
    format_result_text(config, task, result, "filled", "All tracked orders filled.")
}

fn format_partial_filled_body(
    config: &AppConfig,
    task: &TaskConfig,
    result: &ExecutionResult,
) -> String {
    format_result_text(
        config,
        task,
        result,
        "partial_filled",
        "Task completed with partial fills.",
    )
}

fn format_success_html(config: &AppConfig, task: &TaskConfig, result: &ExecutionResult) -> String {
    format_result_html(
        config,
        task,
        result,
        "completed",
        "Task completed successfully.",
    )
}

fn format_failure_html(config: &AppConfig, task: &TaskConfig, error: &str) -> String {
    let timestamp = timestamp_in_config_timezone(config);
    let note_html = format_task_note_html(task);
    let error = html_escape(error);

    format!(
        "<!doctype html><html><body style=\"margin:0;padding:0;background:#f5f7fb;color:#132033;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;\"><div style=\"max-width:760px;margin:0 auto;padding:24px 16px;\"><div style=\"background:#ffffff;border:1px solid #d8e0eb;border-radius:14px;padding:24px;\"><div style=\"font-size:13px;letter-spacing:0.08em;text-transform:uppercase;color:#7a8798;\">Tradebot</div><h1 style=\"margin:8px 0 20px;font-size:24px;line-height:1.3;\">Task failed</h1><table style=\"width:100%;border-collapse:collapse;font-size:14px;\"><tr><td style=\"padding:8px 0;color:#5f6b7a;width:140px;\">Time</td><td style=\"padding:8px 0;\">{timestamp}</td></tr><tr><td style=\"padding:8px 0;color:#5f6b7a;\">Task</td><td style=\"padding:8px 0;\">{task_name}</td></tr><tr><td style=\"padding:8px 0;color:#5f6b7a;\">Broker</td><td style=\"padding:8px 0;\">{broker_name}</td></tr><tr><td style=\"padding:8px 0;color:#5f6b7a;\">Action</td><td style=\"padding:8px 0;\">{action}</td></tr></table>{note_html}<h2 style=\"margin:24px 0 8px;font-size:16px;\">Error</h2><pre style=\"margin:0;padding:16px;background:#fff4f4;border:1px solid #f2c7c7;border-radius:10px;color:#8a1f1f;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;white-space:pre-wrap;\">{error}</pre></div></div></body></html>",
        task_name = html_escape(&task.name),
        broker_name = html_escape(&task.broker),
        action = html_escape(format_action(task.action)),
    )
}

fn format_filled_html(config: &AppConfig, task: &TaskConfig, result: &ExecutionResult) -> String {
    format_result_html(config, task, result, "filled", "All tracked orders filled.")
}

fn format_partial_filled_html(
    config: &AppConfig,
    task: &TaskConfig,
    result: &ExecutionResult,
) -> String {
    format_result_html(
        config,
        task,
        result,
        "partial_filled",
        "Task completed with partial fills.",
    )
}

fn format_task_list_loaded_body(config: &AppConfig, source: &str) -> String {
    let timestamp = timestamp_in_config_timezone(config);
    let counts = task_list_counts(config);
    let active = format_task_summary_lines(&tasks_with_state(&config.tasks, TaskDisplayState::Active));
    let disabled =
        format_task_summary_lines(&tasks_with_state(&config.tasks, TaskDisplayState::Disabled));
    let manual = format_task_summary_lines(&tasks_with_state(&config.tasks, TaskDisplayState::Manual));

    format!(
        "Task list loaded.\n\nSummary\nTime: {timestamp}\nSource: {source}\nEnabled scheduled tasks: {}/{}\n\nActive scheduled tasks\n{active}\n\nDisabled scheduled tasks\n{disabled}\n\nManual tasks\n{manual}",
        counts.enabled, counts.total
    )
}

fn format_task_list_loaded_html(config: &AppConfig, source: &str) -> String {
    let timestamp = timestamp_in_config_timezone(config);
    let counts = task_list_counts(config);
    let active_html =
        format_task_summary_list_html(&tasks_with_state(&config.tasks, TaskDisplayState::Active));
    let disabled_html =
        format_task_summary_list_html(&tasks_with_state(&config.tasks, TaskDisplayState::Disabled));
    let manual_html =
        format_task_summary_list_html(&tasks_with_state(&config.tasks, TaskDisplayState::Manual));

    format!(
        "<!doctype html><html><body style=\"margin:0;padding:0;background:#f5f7fb;color:#132033;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;\"><div style=\"max-width:760px;margin:0 auto;padding:24px 16px;\"><div style=\"background:#ffffff;border:1px solid #d8e0eb;border-radius:14px;padding:24px;\"><div style=\"font-size:13px;letter-spacing:0.08em;text-transform:uppercase;color:#7a8798;\">Tradebot</div><h1 style=\"margin:8px 0 20px;font-size:24px;line-height:1.3;\">Task list loaded</h1><table style=\"width:100%;border-collapse:collapse;font-size:14px;\"><tr><td style=\"padding:8px 0;color:#5f6b7a;width:180px;\">Time</td><td style=\"padding:8px 0;\">{timestamp}</td></tr><tr><td style=\"padding:8px 0;color:#5f6b7a;\">Source</td><td style=\"padding:8px 0;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;\">{source}</td></tr><tr><td style=\"padding:8px 0;color:#5f6b7a;\">Enabled scheduled tasks</td><td style=\"padding:8px 0;\">{enabled}/{total}</td></tr></table><h2 style=\"margin:24px 0 8px;font-size:16px;color:#0f5c44;\">Active scheduled tasks</h2>{active_html}<h2 style=\"margin:24px 0 8px;font-size:16px;color:#7a5d00;\">Disabled scheduled tasks</h2>{disabled_html}<h2 style=\"margin:24px 0 8px;font-size:16px;color:#5f6b7a;\">Manual tasks</h2>{manual_html}</div></div></body></html>",
        timestamp = html_escape(&timestamp),
        source = html_escape(source),
        enabled = counts.enabled,
        total = counts.total,
        active_html = active_html,
        disabled_html = disabled_html,
        manual_html = manual_html,
    )
}

fn format_task_list_changed_body(
    previous: &AppConfig,
    current: &AppConfig,
    source: &str,
    details: &TaskListChangeDetails,
) -> String {
    let timestamp = timestamp_in_config_timezone(current);
    let added = format_changed_task_lines(current, &details.added);
    let removed = format_changed_task_lines(previous, &details.removed);
    let updated = format_updated_task_lines(previous, current, &details.updated);

    format!(
        "Task list changed.\n\nSummary\nTime: {timestamp}\nSource: {source}\nBefore enabled scheduled tasks: {}/{}\nAfter enabled scheduled tasks: {}/{}\nAdded: {}\nRemoved: {}\nUpdated: {}\n\nAdded tasks\n{added}\n\nRemoved tasks\n{removed}\n\nUpdated tasks\n{updated}",
        details.before.enabled,
        details.before.total,
        details.after.enabled,
        details.after.total,
        details.added.len(),
        details.removed.len(),
        details.updated.len(),
    )
}

fn format_task_list_changed_html(
    previous: &AppConfig,
    current: &AppConfig,
    source: &str,
    details: &TaskListChangeDetails,
) -> String {
    let timestamp = timestamp_in_config_timezone(current);
    let added_html = format_changed_task_list_html(current, &details.added);
    let removed_html = format_changed_task_list_html(previous, &details.removed);
    let updated_html = format_updated_task_list_html(previous, current, &details.updated);

    format!(
        "<!doctype html><html><body style=\"margin:0;padding:0;background:#f5f7fb;color:#132033;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;\"><div style=\"max-width:760px;margin:0 auto;padding:24px 16px;\"><div style=\"background:#ffffff;border:1px solid #d8e0eb;border-radius:14px;padding:24px;\"><div style=\"font-size:13px;letter-spacing:0.08em;text-transform:uppercase;color:#7a8798;\">Tradebot</div><h1 style=\"margin:8px 0 20px;font-size:24px;line-height:1.3;\">Task list changed</h1><table style=\"width:100%;border-collapse:collapse;font-size:14px;\"><tr><td style=\"padding:8px 0;color:#5f6b7a;width:180px;\">Time</td><td style=\"padding:8px 0;\">{timestamp}</td></tr><tr><td style=\"padding:8px 0;color:#5f6b7a;\">Source</td><td style=\"padding:8px 0;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;\">{source}</td></tr><tr><td style=\"padding:8px 0;color:#5f6b7a;\">Before enabled scheduled tasks</td><td style=\"padding:8px 0;\">{before_enabled}/{before_total}</td></tr><tr><td style=\"padding:8px 0;color:#5f6b7a;\">After enabled scheduled tasks</td><td style=\"padding:8px 0;\">{after_enabled}/{after_total}</td></tr><tr><td style=\"padding:8px 0;color:#5f6b7a;\">Added</td><td style=\"padding:8px 0;\">{added_count}</td></tr><tr><td style=\"padding:8px 0;color:#5f6b7a;\">Removed</td><td style=\"padding:8px 0;\">{removed_count}</td></tr><tr><td style=\"padding:8px 0;color:#5f6b7a;\">Updated</td><td style=\"padding:8px 0;\">{updated_count}</td></tr></table><h2 style=\"margin:24px 0 8px;font-size:16px;\">Added tasks</h2>{added_html}<h2 style=\"margin:24px 0 8px;font-size:16px;\">Removed tasks</h2>{removed_html}<h2 style=\"margin:24px 0 8px;font-size:16px;\">Updated tasks</h2>{updated_html}</div></div></body></html>",
        timestamp = html_escape(&timestamp),
        source = html_escape(source),
        before_enabled = details.before.enabled,
        before_total = details.before.total,
        after_enabled = details.after.enabled,
        after_total = details.after.total,
        added_count = details.added.len(),
        removed_count = details.removed.len(),
        updated_count = details.updated.len(),
        added_html = added_html,
        removed_html = removed_html,
        updated_html = updated_html,
    )
}

fn format_result_text(
    config: &AppConfig,
    task: &TaskConfig,
    result: &ExecutionResult,
    status: &str,
    headline: &str,
) -> String {
    let timestamp = timestamp_in_config_timezone(config);
    let payload = serde_json::to_string_pretty(result)
        .unwrap_or_else(|_| "{\"error\":\"failed to serialize execution result\"}".into());
    let note_section = format_task_note_section(task);
    let orders_section = format_order_lines_text(&result.orders);
    let warnings_section = format_warning_lines_text(&result.warnings);

    format!(
        "{headline}\n\nSummary\nTime: {timestamp}\nTask: {}\nBroker: {} ({})\nAction: {}\nStatus: {status}\nOrders: {}\nCancellations: {}\nWarnings: {}{note_section}\n\nOrders\n{orders_section}\n\nWarnings\n{warnings_section}\n\nRaw result\n{payload}",
        result.task_name,
        result.broker_name,
        result.broker_kind,
        format_action(result.action),
        result.orders.len(),
        result.cancellations.len(),
        result.warnings.len(),
    )
}

fn format_result_html(
    config: &AppConfig,
    task: &TaskConfig,
    result: &ExecutionResult,
    status: &str,
    headline: &str,
) -> String {
    let timestamp = timestamp_in_config_timezone(config);
    let payload = serde_json::to_string_pretty(result)
        .unwrap_or_else(|_| "{\"error\":\"failed to serialize execution result\"}".into());
    let note_html = format_task_note_html(task);
    let orders_html = format_order_rows_html(&result.orders);
    let warnings_html = format_warning_list_html(&result.warnings);

    format!(
        "<!doctype html><html><body style=\"margin:0;padding:0;background:#f5f7fb;color:#132033;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;\"><div style=\"max-width:760px;margin:0 auto;padding:24px 16px;\"><div style=\"background:#ffffff;border:1px solid #d8e0eb;border-radius:14px;padding:24px;\"><div style=\"font-size:13px;letter-spacing:0.08em;text-transform:uppercase;color:#7a8798;\">Tradebot</div><h1 style=\"margin:8px 0 20px;font-size:24px;line-height:1.3;\">{headline}</h1><table style=\"width:100%;border-collapse:collapse;font-size:14px;\"><tr><td style=\"padding:8px 0;color:#5f6b7a;width:140px;\">Time</td><td style=\"padding:8px 0;\">{timestamp}</td></tr><tr><td style=\"padding:8px 0;color:#5f6b7a;\">Task</td><td style=\"padding:8px 0;\">{task_name}</td></tr><tr><td style=\"padding:8px 0;color:#5f6b7a;\">Broker</td><td style=\"padding:8px 0;\">{broker_name} ({broker_kind})</td></tr><tr><td style=\"padding:8px 0;color:#5f6b7a;\">Action</td><td style=\"padding:8px 0;\">{action}</td></tr><tr><td style=\"padding:8px 0;color:#5f6b7a;\">Status</td><td style=\"padding:8px 0;\"><span style=\"display:inline-block;padding:4px 10px;border-radius:999px;background:#e8f1ff;color:#174ea6;font-size:12px;font-weight:600;\">{status}</span></td></tr><tr><td style=\"padding:8px 0;color:#5f6b7a;\">Orders</td><td style=\"padding:8px 0;\">{orders}</td></tr><tr><td style=\"padding:8px 0;color:#5f6b7a;\">Cancellations</td><td style=\"padding:8px 0;\">{cancellations}</td></tr><tr><td style=\"padding:8px 0;color:#5f6b7a;\">Warnings</td><td style=\"padding:8px 0;\">{warnings}</td></tr></table>{note_html}<h2 style=\"margin:24px 0 8px;font-size:16px;\">Orders</h2><table style=\"width:100%;border-collapse:collapse;font-size:13px;border:1px solid #d8e0eb;border-radius:10px;overflow:hidden;\"><thead><tr style=\"background:#f7f9fc;color:#5f6b7a;text-align:left;\"><th style=\"padding:10px 12px;border-bottom:1px solid #d8e0eb;\">Symbol</th><th style=\"padding:10px 12px;border-bottom:1px solid #d8e0eb;\">Status</th><th style=\"padding:10px 12px;border-bottom:1px solid #d8e0eb;\">Filled</th><th style=\"padding:10px 12px;border-bottom:1px solid #d8e0eb;\">Avg Price</th><th style=\"padding:10px 12px;border-bottom:1px solid #d8e0eb;\">Broker Order ID</th></tr></thead><tbody>{orders_html}</tbody></table><h2 style=\"margin:24px 0 8px;font-size:16px;\">Warnings</h2>{warnings_html}<h2 style=\"margin:24px 0 8px;font-size:16px;\">Raw result</h2><pre style=\"margin:0;padding:16px;background:#0f172a;color:#e2e8f0;border-radius:10px;overflow:auto;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;white-space:pre-wrap;\">{payload}</pre></div></div></body></html>",
        headline = html_escape(headline),
        task_name = html_escape(&result.task_name),
        broker_name = html_escape(&result.broker_name),
        broker_kind = html_escape(&result.broker_kind),
        action = html_escape(format_action(result.action)),
        status = html_escape(status),
        orders = result.orders.len(),
        cancellations = result.cancellations.len(),
        warnings = result.warnings.len(),
        payload = html_escape(&payload),
    )
}

fn format_task_note_section(task: &TaskConfig) -> String {
    task.note
        .as_deref()
        .map(str::trim)
        .filter(|note| !note.is_empty())
        .map(|note| format!("\n\nNote\n{note}"))
        .unwrap_or_default()
}

fn format_task_note_html(task: &TaskConfig) -> String {
    task.note
        .as_deref()
        .map(str::trim)
        .filter(|note| !note.is_empty())
        .map(|note| {
            format!(
                "<h2 style=\"margin:24px 0 8px;font-size:16px;\">Note</h2><div style=\"padding:16px;background:#f7f9fc;border:1px solid #d8e0eb;border-radius:10px;white-space:pre-wrap;\">{}</div>",
                html_escape(note)
            )
        })
        .unwrap_or_default()
}

fn format_order_lines_text(orders: &[OrderResult]) -> String {
    if orders.is_empty() {
        return "- none".into();
    }

    orders
        .iter()
        .map(|order| {
            format!(
                "- {} | status: {} | filled: {} | avg: {} | broker_order_id: {}",
                order_symbol(order),
                order.status,
                format_optional_number(order.filled_qty),
                format_optional_number(order.avg_price),
                order.broker_order_id
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_warning_lines_text(warnings: &[String]) -> String {
    if warnings.is_empty() {
        return "- none".into();
    }

    warnings
        .iter()
        .map(|warning| format!("- {warning}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_order_rows_html(orders: &[OrderResult]) -> String {
    if orders.is_empty() {
        return "<tr><td colspan=\"5\" style=\"padding:12px;color:#5f6b7a;\">No orders</td></tr>"
            .into();
    }

    orders
        .iter()
        .map(|order| {
            format!(
                "<tr><td style=\"padding:10px 12px;border-bottom:1px solid #edf1f5;\">{symbol}</td><td style=\"padding:10px 12px;border-bottom:1px solid #edf1f5;\">{status}</td><td style=\"padding:10px 12px;border-bottom:1px solid #edf1f5;\">{filled}</td><td style=\"padding:10px 12px;border-bottom:1px solid #edf1f5;\">{avg}</td><td style=\"padding:10px 12px;border-bottom:1px solid #edf1f5;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;\">{order_id}</td></tr>",
                symbol = html_escape(order_symbol(order)),
                status = html_escape(&order.status),
                filled = html_escape(&format_optional_number(order.filled_qty)),
                avg = html_escape(&format_optional_number(order.avg_price)),
                order_id = html_escape(&order.broker_order_id),
            )
        })
        .collect::<Vec<_>>()
        .join("")
}

fn format_warning_list_html(warnings: &[String]) -> String {
    if warnings.is_empty() {
        return "<div style=\"padding:16px;background:#f7f9fc;border:1px solid #d8e0eb;border-radius:10px;color:#5f6b7a;\">No warnings</div>".into();
    }

    let items = warnings
        .iter()
        .map(|warning| {
            format!(
                "<li style=\"margin:0 0 8px;\">{}</li>",
                html_escape(warning)
            )
        })
        .collect::<Vec<_>>()
        .join("");
    format!("<ul style=\"margin:0;padding-left:20px;\">{items}</ul>")
}

fn order_symbol(order: &OrderResult) -> &str {
    order
        .raw_metadata
        .get("symbol")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("UNKNOWN")
}

fn format_optional_number(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.4}"))
        .unwrap_or_else(|| "n/a".into())
}

fn format_action(action: TaskAction) -> &'static str {
    match action {
        TaskAction::Place => "place",
        TaskAction::Cancel => "cancel",
    }
}

fn html_escape(raw: impl AsRef<str>) -> String {
    raw.as_ref()
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn task_list_change_details(
    previous: &AppConfig,
    current: &AppConfig,
) -> Option<TaskListChangeDetails> {
    let previous_tasks = previous
        .tasks
        .iter()
        .map(|task| (task.name.clone(), task.clone()))
        .collect::<BTreeMap<_, _>>();
    let current_tasks = current
        .tasks
        .iter()
        .map(|task| (task.name.clone(), task.clone()))
        .collect::<BTreeMap<_, _>>();

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut updated = Vec::new();

    for (name, current_task) in &current_tasks {
        match previous_tasks.get(name) {
            None => added.push(name.clone()),
            Some(previous_task) if previous_task != current_task => {
                updated.push(UpdatedTaskEntry {
                    name: name.clone(),
                    before: pretty_task_json(previous_task),
                    after: pretty_task_json(current_task),
                })
            }
            Some(_) => {}
        }
    }

    for name in previous_tasks.keys() {
        if !current_tasks.contains_key(name) {
            removed.push(name.clone());
        }
    }

    if added.is_empty() && removed.is_empty() && updated.is_empty() {
        return None;
    }

    Some(TaskListChangeDetails {
        before: task_list_counts(previous),
        after: task_list_counts(current),
        added,
        removed,
        updated,
    })
}

fn task_list_counts(config: &AppConfig) -> TaskListCounts {
    TaskListCounts {
        enabled: config
            .tasks
            .iter()
            .filter(|task| {
                task.schedule
                    .as_ref()
                    .is_some_and(|schedule| schedule.enabled)
            })
            .count(),
        total: config.tasks.len(),
    }
}

fn pretty_task_json(task: &TaskConfig) -> String {
    serde_json::to_string_pretty(task)
        .unwrap_or_else(|_| "{\"error\":\"failed to serialize task config\"}".into())
}

fn task_display_state(task: &TaskConfig) -> TaskDisplayState {
    match task.schedule.as_ref() {
        Some(schedule) if schedule.enabled => TaskDisplayState::Active,
        Some(_) => TaskDisplayState::Disabled,
        None => TaskDisplayState::Manual,
    }
}

fn tasks_with_state(tasks: &[TaskConfig], state: TaskDisplayState) -> Vec<TaskConfig> {
    tasks.iter()
        .filter(|task| task_display_state(task) == state)
        .cloned()
        .collect()
}

fn format_task_summary_lines(tasks: &[TaskConfig]) -> String {
    if tasks.is_empty() {
        return "- none".into();
    }

    tasks
        .iter()
        .map(|task| {
            let schedule = task
                .schedule
                .as_ref()
                .map(format_task_schedule_summary)
                .unwrap_or_else(|| "manual".into());
            format!(
                "- [{}] {} | broker={} | action={} | {}",
                task_state_label(task_display_state(task)),
                task.name,
                task.broker,
                format_action(task.action),
                schedule
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_task_summary_list_html(tasks: &[TaskConfig]) -> String {
    if tasks.is_empty() {
        return "<div style=\"padding:16px;background:#f7f9fc;border:1px solid #d8e0eb;border-radius:10px;color:#5f6b7a;\">None</div>".into();
    }

    let items = tasks
        .iter()
        .map(format_task_summary_card_html)
        .collect::<Vec<_>>()
        .join("");
    format!("<div>{items}</div>")
}

fn format_changed_task_lines(config: &AppConfig, names: &[String]) -> String {
    if names.is_empty() {
        return "- none".into();
    }

    names
        .iter()
        .map(|name| {
            config
                .tasks
                .iter()
                .find(|task| task.name == *name)
                .map(format_task_summary_line)
                .unwrap_or_else(|| format!("- {name}"))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_changed_task_list_html(config: &AppConfig, names: &[String]) -> String {
    if names.is_empty() {
        return "<div style=\"padding:16px;background:#f7f9fc;border:1px solid #d8e0eb;border-radius:10px;color:#5f6b7a;\">None</div>".into();
    }

    names
        .iter()
        .map(|name| {
            config
                .tasks
                .iter()
                .find(|task| task.name == *name)
                .map(format_task_summary_card_html)
                .unwrap_or_else(|| {
                    format!(
                        "<div style=\"margin:0 0 12px;padding:14px 16px;background:#f7f9fc;border:1px solid #d8e0eb;border-radius:12px;\">{}</div>",
                        html_escape(name)
                    )
                })
        })
        .collect::<Vec<_>>()
        .join("")
}

fn format_updated_task_lines(
    previous: &AppConfig,
    current: &AppConfig,
    updated: &[UpdatedTaskEntry],
) -> String {
    if updated.is_empty() {
        return "- none".into();
    }

    updated
        .iter()
        .map(|entry| {
            let before_state = previous
                .tasks
                .iter()
                .find(|task| task.name == entry.name)
                .map(task_display_state)
                .unwrap_or(TaskDisplayState::Manual);
            let after_state = current
                .tasks
                .iter()
                .find(|task| task.name == entry.name)
                .map(task_display_state)
                .unwrap_or(TaskDisplayState::Manual);
            format!(
                "- {} | state: {} -> {}\n  Before\n{}\n  After\n{}",
                entry.name,
                task_state_label(before_state),
                task_state_label(after_state),
                entry.before,
                entry.after
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn format_updated_task_list_html(
    previous: &AppConfig,
    current: &AppConfig,
    updated: &[UpdatedTaskEntry],
) -> String {
    if updated.is_empty() {
        return "<div style=\"padding:16px;background:#f7f9fc;border:1px solid #d8e0eb;border-radius:10px;color:#5f6b7a;\">None</div>".into();
    }

    updated
        .iter()
        .map(|entry| {
            let before_state = previous
                .tasks
                .iter()
                .find(|task| task.name == entry.name)
                .map(task_display_state)
                .unwrap_or(TaskDisplayState::Manual);
            let after_state = current
                .tasks
                .iter()
                .find(|task| task.name == entry.name)
                .map(task_display_state)
                .unwrap_or(TaskDisplayState::Manual);
            format!(
                "<div style=\"margin:0 0 16px;padding:16px;background:#f7f9fc;border:1px solid #d8e0eb;border-radius:10px;\"><div style=\"display:flex;justify-content:space-between;gap:12px;align-items:center;margin-bottom:12px;\"><div style=\"font-weight:600;\">{}</div><div><span style=\"{}\">{}</span><span style=\"margin:0 6px;color:#7a8798;\">-></span><span style=\"{}\">{}</span></div></div><div style=\"font-size:12px;color:#5f6b7a;margin-bottom:6px;\">Before</div><pre style=\"margin:0 0 12px;padding:12px;background:#0f172a;color:#e2e8f0;border-radius:10px;overflow:auto;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;white-space:pre-wrap;\">{}</pre><div style=\"font-size:12px;color:#5f6b7a;margin-bottom:6px;\">After</div><pre style=\"margin:0;padding:12px;background:#0f172a;color:#e2e8f0;border-radius:10px;overflow:auto;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;white-space:pre-wrap;\">{}</pre></div>",
                html_escape(&entry.name),
                task_state_badge_style(before_state),
                html_escape(task_state_label(before_state)),
                task_state_badge_style(after_state),
                html_escape(task_state_label(after_state)),
                html_escape(&entry.before),
                html_escape(&entry.after),
            )
        })
        .collect::<Vec<_>>()
        .join("")
}

fn format_task_summary_line(task: &TaskConfig) -> String {
    let schedule = task
        .schedule
        .as_ref()
        .map(format_task_schedule_summary)
        .unwrap_or_else(|| "manual".into());
    format!(
        "- [{}] {} | broker={} | action={} | {}",
        task_state_label(task_display_state(task)),
        task.name,
        task.broker,
        format_action(task.action),
        schedule
    )
}

fn format_task_summary_card_html(task: &TaskConfig) -> String {
    let state = task_display_state(task);
    let schedule = task
        .schedule
        .as_ref()
        .map(format_task_schedule_summary)
        .unwrap_or_else(|| "manual".into());
    format!(
        "<div style=\"margin:0 0 12px;padding:14px 16px;{}\"><div style=\"display:flex;justify-content:space-between;gap:12px;align-items:center;margin-bottom:8px;\"><strong>{}</strong><span style=\"{}\">{}</span></div><div style=\"font-size:13px;color:#2d3a4b;\">broker={} | action={}</div><div style=\"margin-top:6px;font-size:13px;color:#5f6b7a;\">{}</div></div>",
        task_state_card_style(state),
        html_escape(&task.name),
        task_state_badge_style(state),
        html_escape(task_state_label(state)),
        html_escape(&task.broker),
        html_escape(format_action(task.action)),
        html_escape(schedule),
    )
}

fn task_state_label(state: TaskDisplayState) -> &'static str {
    match state {
        TaskDisplayState::Active => "active",
        TaskDisplayState::Disabled => "disabled",
        TaskDisplayState::Manual => "manual",
    }
}

fn task_state_card_style(state: TaskDisplayState) -> &'static str {
    match state {
        TaskDisplayState::Active => {
            "background:#f1fbf6;border:1px solid #b8e3c8;border-radius:12px;"
        }
        TaskDisplayState::Disabled => {
            "background:#fbf7ef;border:1px solid #e8d7a7;border-radius:12px;opacity:0.85;"
        }
        TaskDisplayState::Manual => {
            "background:#f7f9fc;border:1px solid #d8e0eb;border-radius:12px;"
        }
    }
}

fn task_state_badge_style(state: TaskDisplayState) -> &'static str {
    match state {
        TaskDisplayState::Active => {
            "display:inline-block;padding:4px 10px;border-radius:999px;background:#dff5e6;color:#0f5c44;font-size:12px;font-weight:600;"
        }
        TaskDisplayState::Disabled => {
            "display:inline-block;padding:4px 10px;border-radius:999px;background:#f4e6bf;color:#7a5d00;font-size:12px;font-weight:600;"
        }
        TaskDisplayState::Manual => {
            "display:inline-block;padding:4px 10px;border-radius:999px;background:#e9eef5;color:#516174;font-size:12px;font-weight:600;"
        }
    }
}

fn format_task_schedule_summary(schedule: &trading_core::TaskScheduleConfig) -> String {
    let date = schedule.date.as_deref().unwrap_or("any-date");
    let weekdays = if schedule.weekdays.is_empty() {
        "all-days".into()
    } else {
        schedule
            .weekdays
            .iter()
            .map(|weekday| weekday.as_str())
            .collect::<Vec<_>>()
            .join(",")
    };

    format!(
        "{} {} ({}) enabled={} overdue={}",
        date,
        schedule.time,
        weekdays,
        schedule.enabled,
        match schedule.overdue_policy {
            trading_core::ScheduleOverduePolicy::Run => "run",
            trading_core::ScheduleOverduePolicy::Skip => "skip",
        }
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

fn sample_result_for_event(
    task: &TaskConfig,
    event: NotificationEvent,
) -> Result<ExecutionResult, String> {
    if task.action != TaskAction::Place {
        return Err(format!(
            "notification preview for `{event:?}` only supports place tasks"
        ));
    }

    let symbols = if task.symbols.is_empty() {
        vec!["UNKNOWN".to_string()]
    } else {
        task.symbols
            .iter()
            .map(|symbol| symbol.instrument.ticker.clone())
            .collect::<Vec<_>>()
    };

    let orders = match event {
        NotificationEvent::Success => symbols
            .iter()
            .map(|symbol| sample_order_result(symbol, "submitted", None))
            .collect(),
        NotificationEvent::Failure => Vec::new(),
        NotificationEvent::Filled => symbols
            .iter()
            .map(|symbol| sample_order_result(symbol, "filled", Some(1.0)))
            .collect(),
        NotificationEvent::PartialFilled => {
            let mut orders = Vec::new();
            for (index, symbol) in symbols.iter().enumerate() {
                let order = if index == 0 {
                    sample_order_result(symbol, "partially_filled", Some(1.0))
                } else {
                    sample_order_result(symbol, "submitted", None)
                };
                orders.push(order);
            }
            orders
        }
    };

    Ok(ExecutionResult {
        task_name: task.name.clone(),
        broker_name: task.broker.clone(),
        broker_kind: "preview".into(),
        action: task.action,
        orders,
        cancellations: Vec::new(),
        warnings: Vec::new(),
    })
}

fn sample_order_result(symbol: &str, status: &str, filled_qty: Option<f64>) -> OrderResult {
    OrderResult {
        broker_order_id: format!("preview-broker-{symbol}-{status}"),
        client_order_id: format!("preview-client-{symbol}-{status}"),
        status: status.into(),
        filled_qty,
        avg_price: None,
        message: None,
        raw_metadata: json!({ "symbol": symbol }),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use trading_core::{
        AppConfig, DefaultsConfig, ExecutionResult, InstrumentRef, Market, NotificationEvent,
        OrderResult, SymbolTarget, TaskAction, TaskConfig, WatchConfig,
        WatchEmailNotificationConfig, WatchNotificationConfig, WatchNotificationEvent,
    };

    use super::{
        format_task_list_changed_body, format_task_list_changed_html, format_task_list_loaded_body,
        format_task_list_loaded_html, preview_notification, task_list_change_details,
        task_result_is_filled, task_result_is_partially_filled,
    };

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

    #[test]
    fn preview_filled_notification_renders_subject_and_body() {
        let config = AppConfig {
            defaults: DefaultsConfig {
                timezone: "UTC".into(),
                default_tif: trading_core::TimeInForce::Day,
                email: Some(trading_core::EmailTransportConfig {
                    subject_prefix: Some("[stock-trader]".into()),
                }),
            },
            watch: trading_core::WatchConfig::default(),
            brokers: Default::default(),
            tasks: Vec::new(),
        };
        let task = TaskConfig {
            name: "preview".into(),
            broker: "longbridge".into(),
            action: TaskAction::Place,
            note: Some("Send a preview notification with task context.".into()),
            schedule: None,
            execution: None,
            notify: None,
            side: None,
            pricing: None,
            risk: None,
            session: None,
            shared_budget: None,
            time_in_force: None,
            client_tag: None,
            all_open: false,
            symbols: vec![SymbolTarget {
                instrument: InstrumentRef {
                    ticker: "SPY".into(),
                    market: Market::Us,
                    broker_symbol: None,
                    conid: None,
                },
                close_position: false,
                quantity: Some(1),
                amount: None,
                weight: None,
                min_quantity: None,
                quantity_step: None,
                limit_price: None,
                client_order_id: None,
                broker_options: Default::default(),
            }],
        };

        let preview =
            preview_notification(&config, &task, NotificationEvent::Filled, None).unwrap();

        assert_eq!(preview.subject, "[stock-trader] preview filled");
        assert!(preview.body.contains("All tracked orders filled."));
        assert!(preview.body.contains("Summary"));
        assert!(preview.body.contains("Orders"));
        assert!(
            preview
                .body
                .contains("Note\nSend a preview notification with task context.")
        );
        assert!(preview.body.contains("\"status\": \"filled\""));
        assert!(preview.html_body.contains("<table"));
        assert!(preview.html_body.contains("All tracked orders filled."));
    }

    fn sample_watch_config() -> WatchConfig {
        WatchConfig {
            notify: Some(WatchNotificationConfig {
                email: Some(WatchEmailNotificationConfig {
                    to: vec!["ops@example.com".into()],
                    on: vec![
                        WatchNotificationEvent::TaskListLoaded,
                        WatchNotificationEvent::TaskListChanged,
                    ],
                }),
            }),
        }
    }

    #[test]
    fn task_list_change_details_detects_added_removed_and_updated_tasks() {
        let base_task = TaskConfig {
            name: "alpha".into(),
            broker: "paper".into(),
            action: TaskAction::Place,
            note: None,
            schedule: None,
            execution: None,
            notify: None,
            side: None,
            pricing: None,
            risk: None,
            session: None,
            shared_budget: None,
            time_in_force: None,
            client_tag: None,
            all_open: false,
            symbols: vec![],
        };
        let previous = AppConfig {
            defaults: DefaultsConfig::default(),
            watch: sample_watch_config(),
            brokers: Default::default(),
            tasks: vec![
                base_task.clone(),
                TaskConfig {
                    name: "removed".into(),
                    ..base_task.clone()
                },
            ],
        };
        let current = AppConfig {
            defaults: DefaultsConfig::default(),
            watch: sample_watch_config(),
            brokers: Default::default(),
            tasks: vec![
                TaskConfig {
                    client_tag: Some("updated".into()),
                    ..base_task.clone()
                },
                TaskConfig {
                    name: "added".into(),
                    ..base_task
                },
            ],
        };

        let details = task_list_change_details(&previous, &current).unwrap();
        assert_eq!(details.added, vec!["added"]);
        assert_eq!(details.removed, vec!["removed"]);
        assert_eq!(details.updated.len(), 1);
        assert_eq!(details.updated[0].name, "alpha");
    }

    #[test]
    fn task_list_change_details_returns_none_for_identical_tasks() {
        let config = AppConfig {
            defaults: DefaultsConfig::default(),
            watch: sample_watch_config(),
            brokers: Default::default(),
            tasks: vec![],
        };

        assert!(task_list_change_details(&config, &config).is_none());
    }

    #[test]
    fn task_list_loaded_formats_active_disabled_and_manual_sections() {
        let base_task = TaskConfig {
            name: "base".into(),
            broker: "paper".into(),
            action: TaskAction::Place,
            note: None,
            schedule: None,
            execution: None,
            notify: None,
            side: Some(trading_core::OrderSide::Buy),
            pricing: None,
            risk: None,
            session: None,
            shared_budget: None,
            time_in_force: None,
            client_tag: None,
            all_open: false,
            symbols: vec![],
        };
        let config = AppConfig {
            defaults: DefaultsConfig::default(),
            watch: sample_watch_config(),
            brokers: Default::default(),
            tasks: vec![
                TaskConfig {
                    name: "active-task".into(),
                    schedule: Some(trading_core::TaskScheduleConfig {
                        date: None,
                        time: "09:30".into(),
                        weekdays: vec![trading_core::ScheduleWeekday::Mon],
                        enabled: true,
                        overdue_policy: trading_core::ScheduleOverduePolicy::Skip,
                    }),
                    ..base_task.clone()
                },
                TaskConfig {
                    name: "disabled-task".into(),
                    schedule: Some(trading_core::TaskScheduleConfig {
                        date: None,
                        time: "09:30".into(),
                        weekdays: vec![trading_core::ScheduleWeekday::Mon],
                        enabled: false,
                        overdue_policy: trading_core::ScheduleOverduePolicy::Skip,
                    }),
                    ..base_task.clone()
                },
                TaskConfig {
                    name: "manual-task".into(),
                    ..base_task
                },
            ],
        };

        let body = format_task_list_loaded_body(&config, "config/oneshot-tasks/*.toml");
        let html = format_task_list_loaded_html(&config, "config/oneshot-tasks/*.toml");

        assert!(body.contains("Active scheduled tasks"));
        assert!(body.contains("Disabled scheduled tasks"));
        assert!(body.contains("Manual tasks"));
        assert!(body.contains("[active] active-task"));
        assert!(body.contains("[disabled] disabled-task"));
        assert!(body.contains("[manual] manual-task"));

        assert!(html.contains("Active scheduled tasks"));
        assert!(html.contains("Disabled scheduled tasks"));
        assert!(html.contains("Manual tasks"));
        assert!(html.contains(">active<"));
        assert!(html.contains(">disabled<"));
        assert!(html.contains(">manual<"));
    }

    #[test]
    fn task_list_changed_formats_state_transitions() {
        let base_task = TaskConfig {
            name: "task".into(),
            broker: "paper".into(),
            action: TaskAction::Place,
            note: None,
            schedule: None,
            execution: None,
            notify: None,
            side: Some(trading_core::OrderSide::Buy),
            pricing: None,
            risk: None,
            session: None,
            shared_budget: None,
            time_in_force: None,
            client_tag: None,
            all_open: false,
            symbols: vec![],
        };
        let previous = AppConfig {
            defaults: DefaultsConfig::default(),
            watch: sample_watch_config(),
            brokers: Default::default(),
            tasks: vec![TaskConfig {
                schedule: Some(trading_core::TaskScheduleConfig {
                    date: None,
                    time: "09:30".into(),
                    weekdays: vec![trading_core::ScheduleWeekday::Mon],
                    enabled: false,
                    overdue_policy: trading_core::ScheduleOverduePolicy::Skip,
                }),
                ..base_task.clone()
            }],
        };
        let current = AppConfig {
            defaults: DefaultsConfig::default(),
            watch: sample_watch_config(),
            brokers: Default::default(),
            tasks: vec![TaskConfig {
                schedule: Some(trading_core::TaskScheduleConfig {
                    date: None,
                    time: "09:30".into(),
                    weekdays: vec![trading_core::ScheduleWeekday::Mon],
                    enabled: true,
                    overdue_policy: trading_core::ScheduleOverduePolicy::Skip,
                }),
                ..base_task
            }],
        };

        let details = task_list_change_details(&previous, &current).unwrap();
        let body = format_task_list_changed_body(
            &previous,
            &current,
            "config/oneshot-tasks/*.toml",
            &details,
        );
        let html = format_task_list_changed_html(
            &previous,
            &current,
            "config/oneshot-tasks/*.toml",
            &details,
        );

        assert!(body.contains("state: disabled -> active"));
        assert!(html.contains(">disabled<"));
        assert!(html.contains(">active<"));
    }
}
