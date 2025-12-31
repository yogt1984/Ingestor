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

impl ProgressEvent {
    /// Get a human-readable description of the event type
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::Started { .. } => "Started",
            Self::Progress { .. } => "Progress",
            Self::Metric { .. } => "Metric",
            Self::Log { .. } => "Log",
            Self::Completed { .. } => "Completed",
            Self::Error { .. } => "Error",
        }
    }

    /// Check if this is a terminal event (Completed or Error)
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed { .. } | Self::Error { .. })
    }

    /// Get the message from the event (if available)
    pub fn message(&self) -> Option<&str> {
        match self {
            Self::Started { message, .. } => Some(message),
            Self::Progress { message, .. } => Some(message),
            Self::Log { message, .. } => Some(message),
            Self::Completed { message } => Some(message),
            Self::Error { message } => Some(message),
            Self::Metric { .. } => None,
        }
    }
}

impl LogLevel {
    /// Convert to string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
            Self::Debug => "DEBUG",
        }
    }

    /// Check if this is an error level
    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error)
    }

    /// Check if this is a warning or error level
    pub fn is_warning_or_error(&self) -> bool {
        matches!(self, Self::Warn | Self::Error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use tokio::runtime::Runtime;

    // ========================================================================
    // NoOpCallback Tests
    // ========================================================================

    #[test]
    fn test_noop_callback_basic() {
        let callback = NoOpCallback;
        callback.on_event(ProgressEvent::Started {
            total: Some(100),
            message: "Test".to_string(),
        });
        // Should not panic
    }

    #[test]
    fn test_noop_callback_all_event_types() {
        let callback = NoOpCallback;
        
        // Test all event types with NoOpCallback
        callback.on_event(ProgressEvent::Started {
            total: Some(100),
            message: "Starting".to_string(),
        });
        
        callback.on_event(ProgressEvent::Progress {
            current: 50,
            total: Some(100),
            message: "Processing".to_string(),
        });
        
        callback.on_event(ProgressEvent::Metric {
            name: "sharpe".to_string(),
            value: 1.5,
        });
        
        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Info".to_string(),
        });
        
        callback.on_event(ProgressEvent::Completed {
            message: "Done".to_string(),
        });
        
        callback.on_event(ProgressEvent::Error {
            message: "Error".to_string(),
        });
        
        // Should not panic for any event type
    }

    #[test]
    fn test_noop_callback_thread_safety() {
        let callback = Arc::new(NoOpCallback);
        let mut handles = vec![];

        // Spawn multiple threads calling the callback
        for i in 0..10 {
            let callback_clone = callback.clone();
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    callback_clone.on_event(ProgressEvent::Progress {
                        current: i * 100 + j,
                        total: Some(1000),
                        message: format!("Thread {} iteration {}", i, j),
                    });
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread should complete");
        }
        
        // If we get here without panic, thread safety is working
    }

    // ========================================================================
    // LogLevel Tests
    // ========================================================================

    #[test]
    fn test_log_level_all_variants() {
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
    fn test_log_level_as_str() {
        assert_eq!(LogLevel::Info.as_str(), "INFO");
        assert_eq!(LogLevel::Warn.as_str(), "WARN");
        assert_eq!(LogLevel::Error.as_str(), "ERROR");
        assert_eq!(LogLevel::Debug.as_str(), "DEBUG");
    }

    #[test]
    fn test_log_level_is_error() {
        assert!(!LogLevel::Info.is_error());
        assert!(!LogLevel::Warn.is_error());
        assert!(LogLevel::Error.is_error());
        assert!(!LogLevel::Debug.is_error());
    }

    #[test]
    fn test_log_level_is_warning_or_error() {
        assert!(!LogLevel::Info.is_warning_or_error());
        assert!(LogLevel::Warn.is_warning_or_error());
        assert!(LogLevel::Error.is_warning_or_error());
        assert!(!LogLevel::Debug.is_warning_or_error());
    }

    #[test]
    fn test_log_level_equality() {
        assert_eq!(LogLevel::Info, LogLevel::Info);
        assert_ne!(LogLevel::Info, LogLevel::Error);
        assert_eq!(LogLevel::Warn, LogLevel::Warn);
        assert_ne!(LogLevel::Warn, LogLevel::Debug);
    }

    #[test]
    fn test_log_level_copy() {
        let level1 = LogLevel::Info;
        let level2 = level1; // Copy
        assert_eq!(level1, level2);
        assert_eq!(level1, LogLevel::Info);
    }

    // ========================================================================
    // ProgressEvent Tests
    // ========================================================================

    #[test]
    fn test_progress_event_all_variants() {
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

    #[test]
    fn test_progress_event_started_without_total() {
        let event = ProgressEvent::Started {
            total: None,
            message: "Starting".to_string(),
        };
        assert_eq!(event.event_type(), "Started");
        assert!(!event.is_terminal());
        assert_eq!(event.message(), Some("Starting"));
    }

    #[test]
    fn test_progress_event_started_with_total() {
        let event = ProgressEvent::Started {
            total: Some(100),
            message: "Starting".to_string(),
        };
        assert_eq!(event.event_type(), "Started");
        assert!(!event.is_terminal());
        assert_eq!(event.message(), Some("Starting"));
    }

    #[test]
    fn test_progress_event_progress() {
        let event = ProgressEvent::Progress {
            current: 50,
            total: Some(100),
            message: "Processing".to_string(),
        };
        assert_eq!(event.event_type(), "Progress");
        assert!(!event.is_terminal());
        assert_eq!(event.message(), Some("Processing"));
    }

    #[test]
    fn test_progress_event_progress_without_total() {
        let event = ProgressEvent::Progress {
            current: 50,
            total: None,
            message: "Processing".to_string(),
        };
        assert_eq!(event.event_type(), "Progress");
        assert!(!event.is_terminal());
        assert_eq!(event.message(), Some("Processing"));
    }

    #[test]
    fn test_progress_event_metric() {
        let event = ProgressEvent::Metric {
            name: "sharpe".to_string(),
            value: 1.5,
        };
        assert_eq!(event.event_type(), "Metric");
        assert!(!event.is_terminal());
        assert_eq!(event.message(), None); // Metric has no message
    }

    #[test]
    fn test_progress_event_log() {
        let event = ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Info message".to_string(),
        };
        assert_eq!(event.event_type(), "Log");
        assert!(!event.is_terminal());
        assert_eq!(event.message(), Some("Info message"));
    }

    #[test]
    fn test_progress_event_completed() {
        let event = ProgressEvent::Completed {
            message: "Done".to_string(),
        };
        assert_eq!(event.event_type(), "Completed");
        assert!(event.is_terminal());
        assert_eq!(event.message(), Some("Done"));
    }

    #[test]
    fn test_progress_event_error() {
        let event = ProgressEvent::Error {
            message: "Error occurred".to_string(),
        };
        assert_eq!(event.event_type(), "Error");
        assert!(event.is_terminal());
        assert_eq!(event.message(), Some("Error occurred"));
    }

    #[test]
    fn test_progress_event_clone() {
        let event1 = ProgressEvent::Started {
            total: Some(100),
            message: "Test".to_string(),
        };
        let event2 = event1.clone();
        
        assert_eq!(event1.event_type(), event2.event_type());
        assert_eq!(event1.message(), event2.message());
    }

    #[test]
    fn test_progress_event_event_type() {
        assert_eq!(ProgressEvent::Started { total: None, message: "".to_string() }.event_type(), "Started");
        assert_eq!(ProgressEvent::Progress { current: 0, total: None, message: "".to_string() }.event_type(), "Progress");
        assert_eq!(ProgressEvent::Metric { name: "".to_string(), value: 0.0 }.event_type(), "Metric");
        assert_eq!(ProgressEvent::Log { level: LogLevel::Info, message: "".to_string() }.event_type(), "Log");
        assert_eq!(ProgressEvent::Completed { message: "".to_string() }.event_type(), "Completed");
        assert_eq!(ProgressEvent::Error { message: "".to_string() }.event_type(), "Error");
    }

    #[test]
    fn test_progress_event_is_terminal() {
        assert!(!ProgressEvent::Started { total: None, message: "".to_string() }.is_terminal());
        assert!(!ProgressEvent::Progress { current: 0, total: None, message: "".to_string() }.is_terminal());
        assert!(!ProgressEvent::Metric { name: "".to_string(), value: 0.0 }.is_terminal());
        assert!(!ProgressEvent::Log { level: LogLevel::Info, message: "".to_string() }.is_terminal());
        assert!(ProgressEvent::Completed { message: "".to_string() }.is_terminal());
        assert!(ProgressEvent::Error { message: "".to_string() }.is_terminal());
    }

    #[test]
    fn test_progress_event_message() {
        assert_eq!(ProgressEvent::Started { total: None, message: "start".to_string() }.message(), Some("start"));
        assert_eq!(ProgressEvent::Progress { current: 0, total: None, message: "progress".to_string() }.message(), Some("progress"));
        assert_eq!(ProgressEvent::Metric { name: "sharpe".to_string(), value: 1.5 }.message(), None);
        assert_eq!(ProgressEvent::Log { level: LogLevel::Info, message: "log".to_string() }.message(), Some("log"));
        assert_eq!(ProgressEvent::Completed { message: "done".to_string() }.message(), Some("done"));
        assert_eq!(ProgressEvent::Error { message: "error".to_string() }.message(), Some("error"));
    }

    #[test]
    fn test_progress_event_empty_message() {
        let event = ProgressEvent::Started {
            total: None,
            message: String::new(),
        };
        assert_eq!(event.message(), Some(""));
    }

    #[test]
    fn test_progress_event_long_message() {
        let long_message = "A".repeat(10000);
        let event = ProgressEvent::Started {
            total: None,
            message: long_message.clone(),
        };
        assert_eq!(event.message(), Some(long_message.as_str()));
    }

    // ========================================================================
    // TUICallback Tests
    // ========================================================================

    #[tokio::test]
    async fn test_tui_callback_sends_events() {
        let (tx, mut rx) = mpsc::channel(100);
        let callback = TUICallback::new(tx);

        callback.on_event(ProgressEvent::Started {
            total: Some(100),
            message: "Test".to_string(),
        });

        let received = rx.recv().await.expect("Should receive event");
        match received {
            ProgressEvent::Started { total, message } => {
                assert_eq!(total, Some(100));
                assert_eq!(message, "Test");
            }
            _ => panic!("Wrong event type received"),
        }
    }

    #[tokio::test]
    async fn test_tui_callback_multiple_events() {
        let (tx, mut rx) = mpsc::channel(100);
        let callback = TUICallback::new(tx);

        callback.on_event(ProgressEvent::Started {
            total: Some(100),
            message: "Start".to_string(),
        });

        callback.on_event(ProgressEvent::Progress {
            current: 50,
            total: Some(100),
            message: "Progress".to_string(),
        });

        callback.on_event(ProgressEvent::Completed {
            message: "Done".to_string(),
        });

        // Receive all events
        let event1 = rx.recv().await.expect("Should receive first event");
        assert!(matches!(event1, ProgressEvent::Started { .. }));

        let event2 = rx.recv().await.expect("Should receive second event");
        assert!(matches!(event2, ProgressEvent::Progress { .. }));

        let event3 = rx.recv().await.expect("Should receive third event");
        assert!(matches!(event3, ProgressEvent::Completed { .. }));
    }

    #[tokio::test]
    async fn test_tui_callback_all_event_types() {
        let (tx, mut rx) = mpsc::channel(100);
        let callback = TUICallback::new(tx);

        callback.on_event(ProgressEvent::Started {
            total: Some(100),
            message: "Start".to_string(),
        });
        callback.on_event(ProgressEvent::Progress {
            current: 50,
            total: Some(100),
            message: "Progress".to_string(),
        });
        callback.on_event(ProgressEvent::Metric {
            name: "sharpe".to_string(),
            value: 1.5,
        });
        callback.on_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Log".to_string(),
        });
        callback.on_event(ProgressEvent::Completed {
            message: "Done".to_string(),
        });
        callback.on_event(ProgressEvent::Error {
            message: "Error".to_string(),
        });

        // Verify all events received
        let mut count = 0;
        while let Ok(_) = rx.try_recv() {
            count += 1;
        }
        assert_eq!(count, 6);
    }

    #[tokio::test]
    async fn test_tui_callback_full_channel() {
        // Create channel with very small buffer
        let (tx, mut rx) = mpsc::channel(1);
        let callback = TUICallback::new(tx);

        // Fill the channel
        callback.on_event(ProgressEvent::Started {
            total: None,
            message: "First".to_string(),
        });

        // This should not block (try_send will fail silently)
        callback.on_event(ProgressEvent::Progress {
            current: 1,
            total: None,
            message: "Second".to_string(),
        });

        // Should only receive the first event
        let event = rx.recv().await.expect("Should receive first event");
        assert!(matches!(event, ProgressEvent::Started { .. }));

        // Second event should be dropped (channel full)
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_tui_callback_disconnected_channel() {
        let (tx, _rx) = mpsc::channel::<ProgressEvent>(1);
        let callback = TUICallback::new(tx);

        // Drop receiver
        drop(_rx);

        // Should not panic when sending to disconnected channel
        callback.on_event(ProgressEvent::Started {
            total: None,
            message: "Test".to_string(),
        });
    }

    #[tokio::test]
    async fn test_tui_callback_thread_safety() {
        let (tx, mut rx) = mpsc::channel(1000);
        let callback = Arc::new(TUICallback::new(tx));
        let mut handles = vec![];

        // Spawn multiple threads calling the callback
        for i in 0..10 {
            let callback_clone = callback.clone();
            let handle = tokio::spawn(async move {
                for j in 0..100 {
                    callback_clone.on_event(ProgressEvent::Progress {
                        current: i * 100 + j,
                        total: Some(1000),
                        message: format!("Thread {} iteration {}", i, j),
                    });
                }
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.expect("Task should complete");
        }

        // Verify we received all events (or at least some)
        let mut count = 0;
        while let Ok(_) = rx.try_recv() {
            count += 1;
        }
        // Should receive at least some events (may drop some if channel full)
        assert!(count > 0);
    }

    // ========================================================================
    // Helper Function Tests
    // ========================================================================

    #[test]
    fn test_create_noop_callback() {
        let callback = create_noop_callback();
        callback.on_event(ProgressEvent::Started {
            total: None,
            message: "Test".to_string(),
        });
        // Should not panic
    }

    #[tokio::test]
    async fn test_create_tui_callback() {
        let (tx, mut rx) = mpsc::channel(10);
        let callback = create_tui_callback(tx);

        callback.on_event(ProgressEvent::Started {
            total: None,
            message: "Test".to_string(),
        });

        let event = rx.recv().await.expect("Should receive event");
        assert!(matches!(event, ProgressEvent::Started { .. }));
    }

    #[test]
    fn test_create_noop_callback_multiple() {
        // Create multiple no-op callbacks
        let callback1 = create_noop_callback();
        let callback2 = create_noop_callback();

        callback1.on_event(ProgressEvent::Started {
            total: None,
            message: "Test1".to_string(),
        });

        callback2.on_event(ProgressEvent::Started {
            total: None,
            message: "Test2".to_string(),
        });

        // Should not panic
    }

    #[tokio::test]
    async fn test_create_tui_callback_multiple_channels() {
        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);

        let callback1 = create_tui_callback(tx1);
        let callback2 = create_tui_callback(tx2);

        callback1.on_event(ProgressEvent::Started {
            total: None,
            message: "Channel1".to_string(),
        });

        callback2.on_event(ProgressEvent::Started {
            total: None,
            message: "Channel2".to_string(),
        });

        let event1 = rx1.recv().await.expect("Should receive from channel1");
        let event2 = rx2.recv().await.expect("Should receive from channel2");

        match event1 {
            ProgressEvent::Started { message, .. } => assert_eq!(message, "Channel1"),
            _ => panic!("Wrong event type"),
        }

        match event2 {
            ProgressEvent::Started { message, .. } => assert_eq!(message, "Channel2"),
            _ => panic!("Wrong event type"),
        }
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[test]
    fn test_progress_event_with_zero_values() {
        let event = ProgressEvent::Progress {
            current: 0,
            total: Some(0),
            message: "Zero".to_string(),
        };
        assert_eq!(event.event_type(), "Progress");
        assert_eq!(event.message(), Some("Zero"));
    }

    #[test]
    fn test_progress_event_with_large_values() {
        let event = ProgressEvent::Progress {
            current: usize::MAX,
            total: Some(usize::MAX),
            message: "Large".to_string(),
        };
        assert_eq!(event.event_type(), "Progress");
        assert_eq!(event.message(), Some("Large"));
    }

    #[test]
    fn test_progress_event_metric_with_special_values() {
        let events = vec![
            ProgressEvent::Metric {
                name: "zero".to_string(),
                value: 0.0,
            },
            ProgressEvent::Metric {
                name: "negative".to_string(),
                value: -1.5,
            },
            ProgressEvent::Metric {
                name: "infinity".to_string(),
                value: f64::INFINITY,
            },
            ProgressEvent::Metric {
                name: "nan".to_string(),
                value: f64::NAN,
            },
        ];

        for event in events {
            assert_eq!(event.event_type(), "Metric");
            assert_eq!(event.message(), None);
        }
    }

    #[test]
    fn test_progress_event_clone_preserves_data() {
        let original = ProgressEvent::Progress {
            current: 42,
            total: Some(100),
            message: "Test message".to_string(),
        };

        let cloned = original.clone();

        match (original, cloned) {
            (
                ProgressEvent::Progress { current: c1, total: t1, message: m1 },
                ProgressEvent::Progress { current: c2, total: t2, message: m2 },
            ) => {
                assert_eq!(c1, c2);
                assert_eq!(t1, t2);
                assert_eq!(m1, m2);
            }
            _ => panic!("Events should match"),
        }
    }

    #[tokio::test]
    async fn test_tui_callback_high_frequency_events() {
        let (tx, mut rx) = mpsc::channel(1000);
        let callback = TUICallback::new(tx);

        // Send many events rapidly
        for i in 0..1000 {
            callback.on_event(ProgressEvent::Progress {
                current: i,
                total: Some(1000),
                message: format!("Event {}", i),
            });
        }

        // Should receive at least some events (may drop if channel full)
        let mut count = 0;
        while let Ok(_) = rx.try_recv() {
            count += 1;
        }
        assert!(count > 0);
    }

    #[test]
    fn test_progress_callback_trait_send_sync() {
        // Verify that ProgressCallback is Send + Sync
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<NoOpCallback>();
        assert_sync::<NoOpCallback>();

        // TUICallback contains mpsc::Sender which is Send + Sync
        let (tx, _) = mpsc::channel::<ProgressEvent>(1);
        let callback = TUICallback::new(tx);
        assert_send::<TUICallback>();
        assert_sync::<TUICallback>();
    }

    #[test]
    fn test_progress_callback_boxed_send_sync() {
        // Verify that Box<dyn ProgressCallback> is Send + Sync
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        let callback: Box<dyn ProgressCallback> = create_noop_callback();
        assert_send::<Box<dyn ProgressCallback>>();
        assert_sync::<Box<dyn ProgressCallback>>();
    }
}

