use std::sync::Arc;
use tokio::sync::RwLock;
use futures_util::StreamExt;
use tracing::{info, error};
use crate::PriceState;

pub async fn start_binance_feed(
    symbols: &[String],
    price_state: Arc<RwLock<PriceState>>,
) {
    for sym in symbols {
        let ps = price_state.clone();
        let sym = sym.clone();
        tokio::spawn(async move {
            connect_symbol(&sym, ps).await;
        });
    }
    // Keep outer task alive so spawned children are not cancelled
    futures_util::future::pending::<()>().await;
}

async fn connect_symbol(
    symbol: &str,
    price_state: Arc<RwLock<PriceState>>,
) {
    let stream_name = format!("{}usdt@bookTicker", symbol.to_lowercase());
    let url = format!("wss://stream.binance.com:9443/ws/{}", stream_name);

    info!("Connecting to Binance raw stream... URL: {}", url);

    loop {
        match connect_and_listen_raw(&url, symbol, &price_state).await {
            Ok(_) => {}
            Err(e) => error!("Binance WS disconnected for {}: {}, reconnecting in 5s…", symbol, e),
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

async fn connect_and_listen_raw(
    url: &str,
    symbol: &str,
    price_state: &Arc<RwLock<PriceState>>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Connecting to Binance WebSocket raw stream...");
    let (ws, _) = tokio_tungstenite::connect_async(url).await.map_err(|e| {
        error!("Binance connection failed: {}", e);
        e
    })?;
    info!("Connected successfully to raw stream");

    let (_, mut read) = ws.split();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(tungstenite_msg) => {
                if tungstenite_msg.is_ping() || tungstenite_msg.is_pong() {
                    continue;
                }
                let text = match tungstenite_msg.to_text() {
                    Ok(t) => t.to_string(),
                    Err(_) => continue,
                };
                info!("Raw message received: {}", text);

                match serde_json::from_str::<serde_json::Value>(&text) {
                    Ok(resp) => {
                        let sym_from_data = resp["s"].as_str().unwrap_or("").to_uppercase();
                        let expected = format!("{}USDT", symbol.to_uppercase());
                        if sym_from_data != expected {
                            error!("Symbol mismatch: expected {}, got {}", expected, sym_from_data);
                            continue;
                        }
                        if let (Some(bid_str), Some(ask_str)) = (
                            resp["b"].as_str(),
                            resp["a"].as_str(),
                        ) {
                            match (bid_str.parse::<f64>(), ask_str.parse::<f64>()) {
                                (Ok(bid), Ok(ask)) => {
                                    let mid = (bid + ask) / 2.0;
                                    info!("Extracted mid price for {}: bid={}, ask={}, mid={}", symbol, bid, ask, mid);
                                    let mut ps = price_state.write().await;
                                    ps.update_price(symbol, "BINANCE", mid);
                                    info!("State updated for {}", symbol);
                                }
                                (Err(e1), Err(e2)) => {
                                    error!("Failed to parse bid and ask as f64: bid='{}' ({}), ask='{}' ({})", bid_str, e1, ask_str, e2);
                                }
                                (Err(e), _) => {
                                    error!("Failed to parse bid as f64: bid='{}' ({})", bid_str, e);
                                }
                                (_, Err(e)) => {
                                    error!("Failed to parse ask as f64: ask='{}' ({})", ask_str, e);
                                }
                            }
                        } else {
                            error!("Missing 'b' or 'a' fields in response: {}", text);
                        }
                    }
                    Err(e) => {
                        error!("JSON parse error: {} for message: {}", e, text);
                    }
                }
            }
            Err(e) => {
                error!("WebSocket message error: {}", e);
                return Err(Box::new(e));
            }
        }
    }
    error!("WebSocket raw stream ended unexpectedly");
    Err("WebSocket stream ended".into())
}
