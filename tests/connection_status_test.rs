//! Tests for the WebSocket connection status

use ingestor::data::lob_feed_manager::{ConnectionStatus, SharedConnectionStatus};

#[test]
fn test_connection_status_values() {
    assert_eq!(ConnectionStatus::Disconnected as u8, 0);
    assert_eq!(ConnectionStatus::Connecting as u8, 1);
    assert_eq!(ConnectionStatus::Connected as u8, 2);
    assert_eq!(ConnectionStatus::Reconnecting as u8, 3);
}

#[test]
fn test_connection_status_from_u8() {
    assert_eq!(ConnectionStatus::from(0), ConnectionStatus::Disconnected);
    assert_eq!(ConnectionStatus::from(1), ConnectionStatus::Connecting);
    assert_eq!(ConnectionStatus::from(2), ConnectionStatus::Connected);
    assert_eq!(ConnectionStatus::from(3), ConnectionStatus::Reconnecting);
    assert_eq!(ConnectionStatus::from(99), ConnectionStatus::Disconnected); // Unknown defaults to Disconnected
}

#[test]
fn test_shared_connection_status_new() {
    let status = SharedConnectionStatus::new();
    assert_eq!(status.get(), ConnectionStatus::Disconnected);
}

#[test]
fn test_shared_connection_status_default() {
    let status = SharedConnectionStatus::default();
    assert_eq!(status.get(), ConnectionStatus::Disconnected);
}

#[test]
fn test_shared_connection_status_set_get() {
    let status = SharedConnectionStatus::new();

    status.set(ConnectionStatus::Connecting);
    assert_eq!(status.get(), ConnectionStatus::Connecting);

    status.set(ConnectionStatus::Connected);
    assert_eq!(status.get(), ConnectionStatus::Connected);

    status.set(ConnectionStatus::Reconnecting);
    assert_eq!(status.get(), ConnectionStatus::Reconnecting);

    status.set(ConnectionStatus::Disconnected);
    assert_eq!(status.get(), ConnectionStatus::Disconnected);
}

#[test]
fn test_shared_connection_status_clone() {
    let status1 = SharedConnectionStatus::new();
    let status2 = status1.clone();

    // Both should share the same underlying atomic
    status1.set(ConnectionStatus::Connected);
    assert_eq!(status2.get(), ConnectionStatus::Connected);

    status2.set(ConnectionStatus::Reconnecting);
    assert_eq!(status1.get(), ConnectionStatus::Reconnecting);
}

#[test]
fn test_status_line_disconnected() {
    let status = SharedConnectionStatus::new();
    status.set(ConnectionStatus::Disconnected);
    assert_eq!(status.status_line(), "WS: DOWN");
}

#[test]
fn test_status_line_connecting() {
    let status = SharedConnectionStatus::new();
    status.set(ConnectionStatus::Connecting);
    assert_eq!(status.status_line(), "WS: CONNECTING...");
}

#[test]
fn test_status_line_connected() {
    let status = SharedConnectionStatus::new();
    status.set(ConnectionStatus::Connected);
    assert_eq!(status.status_line(), "WS: OK");
}

#[test]
fn test_status_line_reconnecting() {
    let status = SharedConnectionStatus::new();
    status.set(ConnectionStatus::Reconnecting);
    assert_eq!(status.status_line(), "WS: RECONNECTING...");
}

#[test]
fn test_connection_status_equality() {
    assert_eq!(ConnectionStatus::Connected, ConnectionStatus::Connected);
    assert_ne!(ConnectionStatus::Connected, ConnectionStatus::Disconnected);
}

#[test]
fn test_connection_status_debug() {
    let status = ConnectionStatus::Connected;
    let debug_str = format!("{:?}", status);
    assert!(debug_str.contains("Connected"));
}

#[test]
fn test_shared_status_thread_safety() {
    use std::thread;

    let status = SharedConnectionStatus::new();
    let status_clone = status.clone();

    let handle = thread::spawn(move || {
        status_clone.set(ConnectionStatus::Connected);
    });

    handle.join().unwrap();

    // Main thread should see the update
    assert_eq!(status.get(), ConnectionStatus::Connected);
}
