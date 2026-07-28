use std::sync::Arc;
use tokio::sync::RwLock;
use crate::PriceState;

#[derive(Clone)]
pub struct ExecutorConfig {
    pub min_profit_pct: f64,
    pub trade_size_usd: f64,
}

pub async fn execute_swap(
    symbol: &str,
    _amount_in: f64,
    _pool_in: &str,
    _pool_out: &str,
    price_state: &Arc<RwLock<PriceState>>,
    _config: &ExecutorConfig,
) -> Result<(), String> {
    let ps = price_state.read().await;
    let snap = ps.snapshot();
    if let Some(entry) = snap.iter().find(|s| s.symbol == symbol) {
        println!(
            "  Route: {} (BSC {:.4}) -> (BASE {:.4}) | Spread {:.2}%",
            entry.symbol,
            entry.binance_price.unwrap_or(0.0),
            entry.base_price.unwrap_or(0.0),
            entry.spread_pct.unwrap_or(0.0),
        );
    }
    drop(ps);

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    Ok(())
}
