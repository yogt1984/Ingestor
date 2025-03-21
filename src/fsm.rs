
// src/fsm.rs

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// The two possible states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorState {
    Idle,
    Connected,
}

/// Events that trigger state transitions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorEvent {
    Connect,
    Disconnect,
}

/// A minimal state machine with a connected flag
pub struct ConnectorFSM {
    state: ConnectorState,
    pub connected_flag: Arc<AtomicBool>,
}

impl ConnectorFSM {
    pub fn new() -> Self {
        Self {
            state: ConnectorState::Idle,
            connected_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn transition(&mut self, event: ConnectorEvent) {
        use ConnectorEvent::*;
        use ConnectorState::*;

        match (self.state, event) {
            (Idle, Connect) => {
                self.state = Connected;
                self.connected_flag.store(true, Ordering::SeqCst);
                println!("[ConnectorFSM] Idle → Connected");
            }
            (Connected, Disconnect) => {
                self.state = Idle;
                self.connected_flag.store(false, Ordering::SeqCst);
                println!("[ConnectorFSM] Connected → Idle");
            }
            _ => {
                println!(
                    "[BasicFSM] Invalid transition: {:?} + {:?}",
                    self.state, event
                );
            }
        }
    }

    pub fn get_state(&self) -> ConnectorState {
        self.state
    }

    pub fn is_connected(&self) -> bool {
        self.connected_flag.load(Ordering::SeqCst)
    }
}
