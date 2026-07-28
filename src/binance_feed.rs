use std::sync::Arc;
use tokio::sync::RwLock;
use futures_util::StreamExt;
use crate::PriceState;

pub async fn start_binance_feed(
    symbols: &[String],
    price_state: Arc<RwLock<PriceState>>,
) {
    let streams: Vec<String> = symbols
        .iter()
        .map(|s| format!("{}@ticker", s.to_lowercase()))
        .collect();
    let url = format!("wss://stream.binance.com:9443/stream?streams={}", streams.join("/"));

    loop {
        match connect_and_listen(&url, &price_state).await {
            Ok(_) => {},
            Err(e) => eprintln!("Binance WS disconnected: {}, reconnecting in 5s…", e),
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

async fn connect_and_listen(
    url: &str,
    price_state: &Arc<RwLock<PriceState>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (ws, _) = tokio_tungstenite::connect_async(url).await?;
    let (_, mut read) = ws.split();

    while let Some(msg) = read.next().await {
        let text = msg?.to_text()?.to_string();
        if let Ok(resp) = serde_json::from_str::<serde_json::Value>(&text) {
            let data = &resp["data"];
            if let (Some(sym), Some(px)) = (
                data["s"].as_str(),
                data["c"].as_str().and_then(|c| c.parse::<f64>().ok()),
            ) {
                let mut ps = price_state.write().await;
                ps.update_price(&sym.to_uppercase(), "BINANCE", px);
            }
        }
    }
    Err("WebSocket stream ended".into())
}
