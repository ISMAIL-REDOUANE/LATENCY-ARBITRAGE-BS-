use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use futures_util::StreamExt;
use tracing::{info, error};
use crate::PriceState;

pub async fn start_binance_feed(
    symbols: &[String],
    price_state: Arc<RwLock<PriceState>>,
) {
    let binance_pairs: Vec<String> = symbols
        .iter()
        .map(|s| format!("{}USDT", s.to_lowercase()))
        .collect();

    let streams: Vec<String> = binance_pairs
        .iter()
        .map(|s| format!("{}@bookTicker", s))
        .collect();

    let url = format!("wss://stream.binance.com:9443/stream?streams={}", streams.join("/"));

    info!("Connecting to Binance... URL: {}", url);

    let pair_to_symbol: HashMap<String, String> = symbols
        .iter()
        .map(|s| (format!("{}USDT", s.to_uppercase()), s.clone()))
        .collect();

    loop {
        match connect_and_listen(&url, &pair_to_symbol, &price_state).await {
            Ok(_) => {}
            Err(e) => error!("Binance WS disconnected: {}, reconnecting in 5s…", e),
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

async fn connect_and_listen(
    url: &str,
    pair_to_symbol: &HashMap<String, String>,
    price_state: &Arc<RwLock<PriceState>>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Connecting to Binance WebSocket...");
    let (ws, _) = tokio_tungstenite::connect_async(url).await.map_err(|e| {
        error!("Binance connection failed: {}", e);
        e
    })?;
    info!("Connected successfully");
    let (_, mut read) = ws.split();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(tungstenite_msg) => {
                let text = tungstenite_msg.to_text()?.to_string();
                info!("Raw message received: {}", text);

                match serde_json::from_str::<serde_json::Value>(&text) {
                    Ok(resp) => {
                        if let Some(stream_name) = resp["stream"].as_str() {
                            let pair = stream_name.split('@').next().unwrap_or("").to_uppercase();
                            info!("Parsed stream: {}, pair: {}", stream_name, pair);

                            if let Some(sym) = pair_to_symbol.get(&pair) {
                                if let Some(data) = resp["data"].as_object() {
                                    if let (Some(bid_str), Some(ask_str)) = (
                                        data.get("b").and_then(|v| v.as_str()),
                                        data.get("a").and_then(|v| v.as_str()),
                                    ) {
                                        match (bid_str.parse::<f64>(), ask_str.parse::<f64>()) {
                                            (Ok(bid), Ok(ask)) => {
                                                let mid = (bid + ask) / 2.0;
                                                info!("Extracted mid price for {}: bid={}, ask={}, mid={}", sym, bid, ask, mid);
                                                let mut ps = price_state.write().await;
                                                ps.update_price(sym, "BINANCE", mid);
                                                info!("State updated for {}", sym);
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
                                        error!("Missing 'b' or 'a' fields in data: {:?}", data);
                                    }
                                } else {
                                    error!("'data' field is not an object for stream {}", stream_name);
                                }
                            } else {
                                error!("No symbol mapping found for pair {}", pair);
                            }
                        } else {
                            error!("No 'stream' field in response: {}", text);
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
    error!("WebSocket stream ended unexpectedly");
    Err("WebSocket stream ended".into())
}
