use std::sync::Arc;
use tokio::sync::RwLock;
use futures_util::StreamExt;
use tracing::{info, error};
use crate::PriceState;
use alloy::network::Ethereum;
use alloy::providers::{Provider, ProviderBuilder, RootProvider, WsConnect};
use alloy::pubsub::PubSubFrontend;
use alloy::primitives::Address;
use alloy::sol;

sol! {
    #[sol(rpc)]
    interface IUniswapV3Pool {
        function slot0() external view returns (
            uint160 sqrtPriceX96,
            int24 tick,
            uint16 observationIndex,
            uint16 observationCardinality,
            uint16 observationCardinalityNext,
            uint8 feeProtocol,
            bool unlocked
        );
    }
}

const POOLS: &[(&str, &str)] = &[
    ("ETH", "0xd0b53d9277642d899df5c87a3966a349a798f224"),  // WETH/USDC 0.05%
    ("ETH", "0x6c561b446416e1a00e8e93e221854d6ea4171372"),  // WETH/USDC 0.30%
];

pub async fn start_base_feed(
    price_state: Arc<RwLock<PriceState>>,
) {
    let rpc_url = std::env::var("BASE_RPC_URL").unwrap_or_else(|_| {
        "wss://base-rpc.publicnode.com".to_string()
    });

    info!("BASE_RPC_URL resolved to: {}", rpc_url);

    loop {
        match connect_and_listen(&rpc_url, &price_state).await {
            Ok(_) => {}
            Err(e) => error!("Base RPC disconnected: {}, reconnecting in 5s…", e),
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

async fn connect_and_listen(
    rpc_url: &str,
    price_state: &Arc<RwLock<PriceState>>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Connecting to Base RPC... URL: {}", rpc_url);
    let provider: RootProvider<PubSubFrontend, Ethereum> = ProviderBuilder::default()
        .on_ws(WsConnect::new(rpc_url))
        .await
        .map_err(|e| {
            error!("Base RPC WS connection failed: {}", e);
            e
        })?;
    info!("Connected, subscribing to newHeads...");

    let sub = provider.subscribe_blocks().await.map_err(|e| {
        error!("Failed to subscribe to newHeads: {}", e);
        e
    })?;
    let mut stream = sub.into_stream();
    info!("Subscribed to newHeads successfully");

    while let Some(_block) = stream.next().await {
        info!("New block received! Fetching slot0...");
        let mut handles = Vec::new();
        for &(symbol, pool_addr) in POOLS {
            let ps = price_state.clone();
            let prov = provider.clone();
            let pool_address: Address = pool_addr.parse()?;
            let sym = symbol.to_owned();

            handles.push(tokio::spawn(async move {
                let pool = IUniswapV3Pool::new(pool_address, &prov);
                match pool.slot0().call().await {
                    Ok(result) => {
                        let sqrt = result.sqrtPriceX96;
                        let limbs = sqrt.as_limbs();
                        let sqrt_val = (limbs[1] as u128) << 64 | limbs[0] as u128;
                        let sqrt_f64 = sqrt_val as f64;
                        info!("slot0 result: sqrtPriceX96={} (raw u128={})", sqrt, sqrt_val);
                        // WETH is token0 (18 decimals), USDC is token1 (6 decimals)
                        // sqrtPriceX96 = sqrt(token1/token0) * 2^96
                        // price (USDC per WETH) = (sqrtPriceX96 / 2^96)^2 * 10^(18-6)
                        let price = (sqrt_f64 / 2_f64.powi(96)).powi(2) * 1e12;
                        info!("Calculated ETH price: {}", price);
                        let mut state = ps.write().await;
                        state.update_price(&sym, "BASE", price);
                        info!("State updated for ETH on BASE with price={}", price);
                    }
                    Err(e) => {
                        error!("Error fetching slot0 for pool {}: {}", sym, e);
                    }
                }
            }));
        }
        for h in handles {
            let _ = h.await;
        }
    }

    error!("Base RPC stream ended unexpectedly");
    Err("Base RPC stream ended".into())
}
