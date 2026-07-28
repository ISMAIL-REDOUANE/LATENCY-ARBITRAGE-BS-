mod binance_feed;
mod base_feed;
mod dashboard;
mod executor;
mod telegram;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::Serialize;
use std::time::Instant;

/// Per-symbol price snapshot from both venues
#[derive(Debug, Clone, Serialize)]
pub struct SymbolSnapshot {
    pub symbol: String,
    pub binance_price: Option<f64>,
    pub base_price: Option<f64>,
    pub spread_pct: Option<f64>,
}

/// Global price state — one entry per symbol, each with source-level prices
#[derive(Debug)]
pub struct PriceState {
    prices: HashMap<String, HashMap<String, f64>>,
    timestamps: HashMap<String, Instant>,
}

impl PriceState {
    pub fn new() -> Self {
        Self {
            prices: HashMap::new(),
            timestamps: HashMap::new(),
        }
    }

    pub fn update_price(&mut self, symbol: &str, exchange: &str, price: f64) {
        self.prices
            .entry(symbol.to_string())
            .or_default()
            .insert(exchange.to_string(), price);
        self.timestamps.insert(symbol.to_string(), Instant::now());
    }

    /// Return snapshot for every symbol that has at least one price.
    pub fn snapshot(&self) -> Vec<SymbolSnapshot> {
        let mut out: Vec<SymbolSnapshot> = self
            .prices
            .iter()
            .map(|(symbol, sources)| {
                let binance = sources.get("BINANCE").copied();
                let base = sources.get("BASE").copied();
                let spread = match (binance, base) {
                    (Some(b), Some(a)) if a != 0.0 && b != 0.0 => {
                        Some((b - a) / a * 100.0)
                    }
                    _ => None,
                };
                SymbolSnapshot {
                    symbol: symbol.clone(),
                    binance_price: binance,
                    base_price: base,
                    spread_pct: spread,
                }
            })
            .collect();
        out.sort_by(|a, b| {
            b.spread_pct
                .unwrap_or(f64::NEG_INFINITY)
                .partial_cmp(&a.spread_pct.unwrap_or(f64::NEG_INFINITY))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        out
    }

    pub fn best_arb(&self) -> Option<SymbolSnapshot> {
        self.snapshot().into_iter().next()
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let symbols = vec![
        "SUSDE".into(),
        "USDC".into(),
        "DAI".into(),
        "USDT".into(),
        "GHO".into(),
        "EURC".into(),
        "ETH".into(),
        "CBETH".into(),
        "RETH".into(),
    ];

    let price_state = Arc::new(RwLock::new(PriceState::new()));

    // WebSocket feed
    let ps_binance = price_state.clone();
    let syms = symbols.clone();
    tokio::spawn(async move {
        binance_feed::start_binance_feed(&syms, ps_binance).await;
    });

    // On-chain feed
    let ps_base = price_state.clone();
    tokio::spawn(async move {
        base_feed::start_base_feed(ps_base).await;
    });

    // Dashboard
    let app_state = Arc::new(dashboard::AppState::new());
    let dash_state = app_state.clone();
    tokio::spawn(async move {
        dashboard::run_dashboard(dash_state).await;
    });

    let cfg = executor::ExecutorConfig {
        min_profit_pct: 0.1,
        trade_size_usd: 100.0,
    };

    // Main loop: every 3 s, snapshot, alert if arb, push to dashboard
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let pairs = {
            let ps = price_state.read().await;
            ps.snapshot()
        };

        // Send snapshot to dashboard
        dashboard::send_price_update(&app_state, pairs.clone()).await;

        // Alert if spread exceeds threshold
        let best_pair = pairs.first().cloned();
        if let Some(best) = best_pair {
            if let Some(spread) = best.spread_pct {
                if spread > cfg.min_profit_pct {
                    let msg = format!(
                        "🚀 <b>{}</b> spread: <b>{:.2}%</b><br>  BSC: {:.4} | BASE: {:.4}",
                        best.symbol, spread,
                        best.binance_price.unwrap_or(0.0),
                        best.base_price.unwrap_or(0.0),
                    );
                    telegram::send_alert(&msg);

                    let sym = best.symbol.clone();
                    let cfg2 = cfg.clone();
                    let ps = price_state.clone();
                    tokio::spawn(async move {
                        let _ = executor::execute_swap(
                            &sym,
                            1.0,
                            "pool_placeholder",
                            "pool_placeholder",
                            &ps,
                            &cfg2,
                        )
                        .await;
                    });
                }
            }
        }
    }
}
