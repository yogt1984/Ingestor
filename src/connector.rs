use crate::fsm::{ConnectorFSM, ConnectorState, ConnectorEvent};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::StreamExt;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use std::sync::Arc;
use log::{info, warn, error, debug};
use tokio::task;
use serde::Deserialize;


pub struct LobConnector {
    lob_uri: String,
    fsm: ConnectorFSM,
}

impl LobConnector {
    pub fn new(uri: String) -> Self {
        Self {
            lob_uri: uri,
            fsm: ConnectorFSM::new(),
        }
    }

    pub fn get_state(&self) -> crate::fsm::ConnectorState {
        self.fsm.get_state()
    }

    pub async fn run(&mut self) {
        let mut retry_delay = Duration::from_secs(1);

        loop {
            match connect_async(&self.lob_uri).await {
                Ok((ws_stream, _)) => {
                    info!("Connected to LOB WebSocket at {}", self.lob_uri);
                    self.fsm.transition(ConnectorEvent::Connect);
                    let (_, mut read) = ws_stream.split();

                    while let Some(message_result) = read.next().await {
                        match message_result {
                            Ok(Message::Text(text)) => {
                                debug!("📥 LOB UPDATE (text): {}", text);
                            }
                            Ok(Message::Binary(bin)) => {
                                if let Ok(text) = String::from_utf8(bin) {
                                    debug!("📥 LOB UPDATE (binary): {}", text);
                                } else {
                                    warn!("Received non-UTF8 binary message.");
                                }
                            }
                            Ok(_) => {} // Ping, Pong, Close, etc.
                            Err(err) => {
                                error!("WebSocket error: {}", err);
                                break;
                            }
                        }
                    }
                    
                    // End of stream (client disconnected or dropped)
                    self.fsm.transition(ConnectorEvent::Disconnect);
                    warn!("LOB WebSocket stream closed.");
                }

                Err(err) => {
                    error!("Failed to connect to LOB WebSocket: {}", err);
                }
            }

            warn!("Reconnecting in {:?}...", retry_delay);
            sleep(retry_delay).await;
            retry_delay = std::cmp::min(retry_delay * 2, Duration::from_secs(60));
        }
    }
}

#[derive(Debug, Deserialize)]
struct TradeUpdate {
    e: String,
    E: u64,
    s: String,
    t: u64,
    p: String,
    q: String,
    T: u64,
    m: bool,
    M: bool,
}

pub struct TradesConnector {
    trades_uri: String,
    fsm: ConnectorFSM,
}

impl TradesConnector 
{
    pub fn new(uri: String) -> Self {
        Self {
            trades_uri: uri,
            fsm: ConnectorFSM::new(),
        }
    }

    pub fn get_state(&self) -> crate::fsm::ConnectorState {
        self.fsm.get_state()
    }

    pub async fn run(&mut self) {
        let mut retry_delay = Duration::from_secs(1);
    
        loop {
            match connect_async(&self.trades_uri).await {
                Ok((ws_stream, _)) => {
                    info!("Connected to Trade WebSocket");
                    self.fsm.transition(ConnectorEvent::Connect);
    
                    let (_, mut read) = ws_stream.split();
    
                    while let Some(message_result) = read.next().await {
                        match message_result {
                            Ok(Message::Text(text)) => {
                                let start = Instant::now();
    
                                match serde_json::from_str::<TradeUpdate>(&text) {
                                    Ok(trade_update) => {
                                        if let (Ok(price), Ok(qty)) = (
                                            trade_update.p.parse::<f64>(),
                                            trade_update.q.parse::<f64>(),
                                        ) {
                                            debug!(
                                                "📊 TRADE: price = {:.2}, quantity = {:.4}, time = {}, buyer_maker = {}",
                                                price,
                                                qty,
                                                trade_update.T,
                                                trade_update.m
                                            );
    
                                            // Update state
                                            //market_state
                                            //    .update_trades(
                                            //        price,
                                            //        qty,
                                            //        trade_update.T as i64,
                                            //        trade_update.m,
                                            //    )
                                            //    .await;
                                        } else {
                                            warn!(
                                                "Trade WebSocket: Invalid price or quantity in message: {}",
                                                text
                                            );
                                        }
    
                                        debug!("Trade ingestion completed in {:?}", start.elapsed());
                                    }
                                    Err(err) => {
                                        error!(
                                            "Trade WebSocket: Failed to parse message: {}\nError: {}",
                                            text, err
                                        );
                                    }
                                }
                            }
                            Ok(Message::Binary(bin)) => {
                                if let Ok(text) = String::from_utf8(bin) {
                                    debug!("Trade Message (binary): {}", text);
                                }
                            }
                            Ok(_) => {}
                            Err(err) => {
                                error!("WebSocket error: {}", err);
                                break;
                            }
                        }
                    }
    
                    self.fsm.transition(ConnectorEvent::Disconnect);
                    warn!("Trade WebSocket stream closed.");
                }
    
                Err(err) => {
                    error!("Failed to connect to Trade WebSocket: {}", err);
                }
            }
    
            warn!("Reconnecting in {:?}...", retry_delay);
            sleep(retry_delay).await;
            retry_delay = std::cmp::min(retry_delay * 2, Duration::from_secs(60));
        }
    }
}