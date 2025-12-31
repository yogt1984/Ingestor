//! Common types and utilities for command execution
//!
//! This module provides shared types used across all command modules, including
//! progress callbacks, event types, and common utilities.

use std::sync::Arc;
use tokio::sync::mpsc;

/// Log level for progress events
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Info,
    Warn,
    Error,
    Debug,
}

/// Progress event emitted during command execution
#[derive(Debug, Clone)]
pub enum ProgressEvent {
    /// Command execution started
    Started {
        /// Total number of items to process (if known)
        total: Option<usize>,
        /// Initial message
        message: String,
    },
    /// Progress update
    Progress {
        /// Current item number
        current: usize,
        /// Total items (if known)
        total: Option<usize>,
        /// Progress message
        message: String,
    },
    /// Metric update (e.g., current Sharpe ratio during grid search)
    Metric {
        /// Metric name
        name: String,
        /// Metric value
        value: f64,
    },
    /// Log message
    Log {
        /// Log level
        level: LogLevel,
        /// Log message
        message: String,
    },
    /// Command execution completed
    Completed {
        /// Success message
        message: String,
    },
    /// Command execution error
    Error {
        /// Error message
        message: String,
    },
}

/// Trait for receiving progress updates during command execution
///
/// Both CLI and TUI can implement this trait to receive real-time updates
/// from long-running commands.
pub trait ProgressCallback: Send + Sync {
    /// Called when a progress event occurs
    fn on_event(&self, event: ProgressEvent);
}

/// No-op callback implementation for CLI (default)
///
/// This callback does nothing and is used by default in CLI binaries
/// where progress is typically shown via progress bars or simple print statements.
pub struct NoOpCallback;

impl ProgressCallback for NoOpCallback {
    fn on_event(&self, _event: ProgressEvent) {
        // No-op: do nothing
    }
}

/// TUI callback implementation
///
/// This callback sends progress events to a channel that the TUI can consume.
/// Created in the TUI integration layer.
pub struct TUICallback {
    tx: mpsc::Sender<ProgressEvent>,
}

impl TUICallback {
    /// Create a new TUI callback with the given channel sender
    pub fn new(tx: mpsc::Sender<ProgressEvent>) -> Self {
        Self { tx }
    }
}

impl ProgressCallback for TUICallback {
    fn on_event(&self, event: ProgressEvent) {
        // Try to send, but don't block if channel is full
        let _ = self.tx.try_send(event);
    }
}

/// Helper to create a boxed callback from a channel
pub fn create_tui_callback(tx: mpsc::Sender<ProgressEvent>) -> Box<dyn ProgressCallback> {
    Box::new(TUICallback::new(tx))
}

/// Helper to create a no-op callback
pub fn create_noop_callback() -> Box<dyn ProgressCallback> {
    Box::new(NoOpCallback)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_callback() {
        let callback = NoOpCallback;
        callback.on_event(ProgressEvent::Started {
            total: Some(100),
            message: "Test".to_string(),
        });
        // Should not panic
    }

    #[test]
    fn test_log_level() {
        let levels = [
            LogLevel::Info,
            LogLevel::Warn,
            LogLevel::Error,
            LogLevel::Debug,
        ];
        for level in levels {
            assert!(matches!(level, LogLevel::Info | LogLevel::Warn | LogLevel::Error | LogLevel::Debug));
        }
    }

    #[test]
    fn test_progress_event_variants() {
        let events = vec![
            ProgressEvent::Started {
                total: Some(100),
                message: "Starting".to_string(),
            },
            ProgressEvent::Progress {
                current: 50,
                total: Some(100),
                message: "Processing".to_string(),
            },
            ProgressEvent::Metric {
                name: "sharpe".to_string(),
                value: 1.5,
            },
            ProgressEvent::Log {
                level: LogLevel::Info,
                message: "Info message".to_string(),
            },
            ProgressEvent::Completed {
                message: "Done".to_string(),
            },
            ProgressEvent::Error {
                message: "Error occurred".to_string(),
            },
        ];

        // Verify all variants can be created
        assert_eq!(events.len(), 6);
    }
}

