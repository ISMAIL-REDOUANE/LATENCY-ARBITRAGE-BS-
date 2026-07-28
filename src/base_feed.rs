use std::sync::Arc;
use tokio::sync::RwLock;
use crate::PriceState;

const POOLS: &[(&str, &str, &str, &str)] = &[
    // (symbol, token_a, token_b, pool_address)
    ("SUSDE", "0x5C5b196aB0C7AcC0f9089C5BE1bA6cB5C6b7A8c9", "0x0000000000000000000000000000000000000000", "0x1234567890abcdef1234567890abcdef12345678"),
    ("USDC",  "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", "0x0000000000000000000000000000000000000000", "0x2345678901abcdef2345678901abcdef23456789"),
    ("DAI",   "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb", "0x0000000000000000000000000000000000000000", "0x3456789012abcdef3456789012abcdef34567890"),
    ("USDT",  "0xfde4C96cA859bE7bE2F19a5360E40Bb9d3a6A0d7", "0x0000000000000000000000000000000000000000", "0x4567890123abcdef4567890123abcdef45678901"),
    ("GHO",   "0x7Dc3B0b6A1eE0A3c9B2A8f1d4E5F6a7B8c9D0E1", "0x0000000000000000000000000000000000000000", "0x5678901234abcdef5678901234abcdef56789012"),
    ("EURC",  "0x60a3E35Cc8b750E3C5c2C0B2b7E9F1d4C8b6A7e", "0x0000000000000000000000000000000000000000", "0x6789012345abcdef6789012345abcdef67890123"),
    ("ETH",   "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE", "0x0000000000000000000000000000000000000000", "0x7890123456abcdef7890123456abcdef78901234"),
    ("CBETH", "0x2Ae3F1Ec7F1FfB2b5b3b4c5d6e7f8a9b0c1d2e3", "0x0000000000000000000000000000000000000000", "0x8901234567abcdef8901234567abcdef89012345"),
    ("RETH",  "0x3Bf4C2D8e9F0a1b2c3d4e5f6a7b8c9d0e1f2a3", "0x0000000000000000000000000000000000000000", "0x9012345678abcdef9012345678abcdef90123456"),
];

pub async fn start_base_feed(
    price_state: Arc<RwLock<PriceState>>,
) {
    loop {
        let mut futures = Vec::new();
        for &(symbol, token_a, token_b, pool) in POOLS {
            let ps = price_state.clone();
            let sym = symbol.to_owned();
            let a = token_a.to_owned();
            let b = token_b.to_owned();
            let p = pool.to_owned();
            futures.push(tokio::spawn(async move {
                fetch_pool_price(&sym, &a, &b, &p, &ps).await;
            }));
        }
        for f in futures {
            let _ = f.await;
        }
        tokio::time::sleep(std::time::Duration::from_secs(12)).await;
    }
}

async fn fetch_pool_price(
    symbol: &str,
    _token_a: &str,
    _token_b: &str,
    _pool: &str,
    price_state: &Arc<RwLock<PriceState>>,
) {
    let dummy_price = 1.0 + fastrand::f64() * 0.01 - 0.005;
    let mut ps = price_state.write().await;
    ps.update_price(symbol, "BASE", dummy_price);
}
