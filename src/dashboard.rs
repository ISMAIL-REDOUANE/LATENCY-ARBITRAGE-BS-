use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, Mutex};
use axum::{
    Router,
    routing::get,
    response::IntoResponse,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State,
};
use serde::Serialize;
use tower_http::services::ServeDir;
use tracing::{info, error};
use std::net::SocketAddr;

use crate::SymbolSnapshot;

#[derive(Debug, Clone, Serialize)]
pub struct TradeInfo {
    pub side: String,
    pub price: f64,
    pub quantity: f64,
    pub profit: f64,
    pub timestamp: u64,
}

pub struct TradeStats {
    pub total_profit: f64,
    pub total_trades: u64,
    pub last_trade: Option<TradeInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardData {
    pub pairs: Vec<SymbolSnapshot>,
    pub total_profit: f64,
    pub total_trades: u64,
    pub last_trade: Option<TradeInfo>,
    pub timestamp: u64,
}

pub struct AppState {
    pub broadcast_tx: broadcast::Sender<String>,
    pub trade_stats: Arc<Mutex<TradeStats>>,
}

impl AppState {
    pub fn new() -> Self {
        let (broadcast_tx, _) = broadcast::channel(256);
        Self {
            broadcast_tx,
            trade_stats: Arc::new(Mutex::new(TradeStats {
                total_profit: 0.0,
                total_trades: 0,
                last_trade: None,
            })),
        }
    }
}

pub async fn send_price_update(app_state: &AppState, pairs: Vec<SymbolSnapshot>) {
    if pairs.is_empty() {
        tracing::warn!("send_price_update: pairs is EMPTY — no data from feeds yet");
    }
    info!("send_price_update: sending {} pairs to frontend", pairs.len());
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let stats = app_state.trade_stats.lock().await;
    let san = |v: Option<f64>| v.filter(|f| f.is_finite());
    let pairs: Vec<SymbolSnapshot> = pairs
        .into_iter()
        .map(|p| SymbolSnapshot {
            binance_price: san(p.binance_price),
            base_price: san(p.base_price),
            spread_pct: san(p.spread_pct),
            ..p
        })
        .collect();
    let data = DashboardData {
        total_profit: stats.total_profit,
        total_trades: stats.total_trades,
        last_trade: stats.last_trade.clone(),
        timestamp: ts,
        pairs,
    };
    drop(stats);

    let json = serde_json::to_string(&data).expect("dashboard JSON serialization failed");
    let _ = app_state.broadcast_tx.send(json);
}

pub async fn run_dashboard(state: Arc<AppState>) {
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .fallback_service(ServeDir::new("static").append_index_html_on_directories(true))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    info!("Dashboard listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Failed to bind dashboard port 8080");

    if let Err(e) = axum::serve(listener, app).await {
        error!("Dashboard server error: {e}");
    }
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    let mut rx = state.broadcast_tx.subscribe();

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(msg) => {
                        if socket.send(Message::Text(msg)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(_)) => {}
                    _ => break,
                }
            }
        }
    }
}
