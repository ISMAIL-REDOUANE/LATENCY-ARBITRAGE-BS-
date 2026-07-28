use alloy::primitives::U256;
use alloy::providers::Provider;
use alloy::network::Ethereum;
use alloy::transports::Transport;
use alloy::sol;
use tracing::{info, error};

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function allowance(address owner, address spender) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
    }
}

pub async fn ensure_approval<T, P>(
    provider: &P,
    owner: alloy::primitives::Address,
    token: alloy::primitives::Address,
    spender: alloy::primitives::Address,
    amount: U256,
) where
    T: Transport + Clone,
    P: Provider<T, Ethereum>,
{
    let token_contract = IERC20::new(token, provider);

    let current = token_contract
        .allowance(owner, spender)
        .call()
        .await
        .map(|r| r._0)
        .unwrap_or(U256::ZERO);

    info!(?token, ?spender, %current, "Current allowance");

    if current >= amount {
        info!(?token, "Allowance already sufficient");
        return;
    }

    info!(?token, ?spender, "Approving infinite allowance");
    match token_contract.approve(spender, U256::MAX).send().await {
        Ok(pending) => {
            info!("Approval tx sent: {:?}", pending.tx_hash());
            match pending.get_receipt().await {
                Ok(receipt) => {
                    if receipt.status() {
                        info!(?token, ?spender, "Approval confirmed");
                    } else {
                        error!(?token, "Approval tx reverted");
                    }
                }
                Err(e) => error!(?token, "Approval receipt error: {e}"),
            }
        }
        Err(e) => error!(?token, "Approval send error: {e}"),
    }
}
