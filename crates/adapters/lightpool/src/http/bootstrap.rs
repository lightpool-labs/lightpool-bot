// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! Create / mint event contracts and wait for clob-index indexing.

use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use lightpool_sdk::{
    ActionBuilder, ContractAddress, CreateEventContractParams, MintEventContractParams, Signer,
    TransactionBuilder, extract_event_contract_created_from_events, parse_token_contract,
};
use lightpool_sdk::event_contract_events::EventContractCreatedEvent;

use super::clob_index::ClobIndexHttpClient;
use super::models::Market;

/// Result of creating and minting one LightPool event market.
#[derive(Debug, Clone)]
pub struct BootstrappedMarket {
    pub question: String,
    pub market_address: String,
    pub yes_token: String,
    pub no_token: String,
    pub slug: String,
}

pub async fn create_event_contract(
    client: &ClobIndexHttpClient,
    signer: &Signer,
    question: &str,
    collateral_token: &str,
    resolution_deadline: u64,
    tick_size: u64,
    min_order_size: u64,
    maker_fee_bps: u16,
    taker_fee_bps: u16,
    allow_market_orders: bool,
) -> anyhow::Result<EventContractCreatedEvent> {
    let collateral = parse_token_contract(collateral_token.trim())
        .map_err(|e| anyhow::anyhow!("invalid collateral token: {e}"))?;
    let oracle = signer.address();
    let params = CreateEventContractParams {
        question: question.to_string(),
        oracle,
        collateral_token: collateral,
        resolution_deadline,
        tick_size,
        min_order_size,
        maker_fee_bps,
        taker_fee_bps,
        allow_market_orders,
        neg_risk_group_id: None,
    };

    let action = ActionBuilder::create_event_contract(params)
        .map_err(|e| anyhow::anyhow!("build create_event_contract: {e}"))?;
    let tx = TransactionBuilder::new()
        .sender(signer.address())
        .expiration(u64::MAX)
        .add_action(action)
        .build_and_sign_only(signer)
        .map_err(|e| anyhow::anyhow!("sign create_event_contract: {e}"))?;

    let response = client.submit_transaction(tx).await?;
    if !response.receipt.is_success() {
        bail!(
            "create_event_contract failed: {:?}",
            response.receipt.status
        );
    }

    extract_event_contract_created_from_events(&response.receipt)
        .context("event_contract_created missing from receipt")
}

pub async fn mint_event_contract(
    client: &ClobIndexHttpClient,
    signer: &Signer,
    market_address: ContractAddress,
    amount: u64,
    collateral_token: ContractAddress,
    yes_token: ContractAddress,
    no_token: ContractAddress,
) -> anyhow::Result<()> {
    let params = MintEventContractParams {
        amount,
        collateral_token,
        yes_token,
        no_token,
    };
    let action = ActionBuilder::mint_event_contract(market_address, params)
        .map_err(|e| anyhow::anyhow!("build mint_event_contract: {e}"))?;
    let tx = TransactionBuilder::new()
        .sender(signer.address())
        .expiration(u64::MAX)
        .add_action(action)
        .build_and_sign_only(signer)
        .map_err(|e| anyhow::anyhow!("sign mint_event_contract: {e}"))?;

    let response = client.submit_transaction(tx).await?;
    if !response.receipt.is_success() {
        bail!("mint_event_contract failed: {:?}", response.receipt.status);
    }
    Ok(())
}

pub async fn wait_for_market_by_address(
    client: &ClobIndexHttpClient,
    market_address: &str,
    timeout: Duration,
) -> anyhow::Result<Market> {
    let deadline = Instant::now() + timeout;
    let mut last_err = None;
    while Instant::now() < deadline {
        match client.fetch_markets_by_addresses(&[market_address.to_string()]).await {
            Ok(markets) => {
                if let Some(market) = markets
                    .into_iter()
                    .find(|m| m.market_address.eq_ignore_ascii_case(market_address))
                {
                    return Ok(market);
                }
            }
            Err(e) => last_err = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    match last_err {
        Some(e) => Err(e).context(format!(
            "timed out waiting for market {market_address} in clob-index"
        )),
        None => bail!("timed out waiting for market {market_address} in clob-index"),
    }
}

/// Create, mint, and wait for indexing of one market.
pub async fn bootstrap_one_market(
    client: &ClobIndexHttpClient,
    signer: &Signer,
    question: &str,
    collateral_token: &str,
    resolution_deadline: u64,
    mint_amount: u64,
) -> anyhow::Result<BootstrappedMarket> {
    let created = create_event_contract(
        client,
        signer,
        question,
        collateral_token,
        resolution_deadline,
        1_000,   // 0.001
        100_000, // 0.1
        10,
        20,
        true,
    )
    .await?;

    mint_event_contract(
        client,
        signer,
        created.market_address,
        mint_amount,
        created.collateral_token,
        created.yes_token,
        created.no_token,
    )
    .await?;

    let market_address = created.market_address.to_string();
    let indexed = wait_for_market_by_address(client, &market_address, Duration::from_secs(30))
        .await
        .with_context(|| format!("index wait for {market_address}"))?;

    Ok(BootstrappedMarket {
        question: question.to_string(),
        market_address,
        yes_token: created.yes_token.to_string(),
        no_token: created.no_token.to_string(),
        slug: indexed.slug,
    })
}
