// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use anyhow::Context;
use lightpool_sdk::Signer;

/// Build a signer from a private key string.
///
/// Accepts the same formats as `lightpool-cli` wallet files:
/// - 32-byte hex (64 hex chars, optional `0x` prefix)
/// - SDK base64-encoded secret key
pub fn signer_from_private_key(private_key: &str) -> anyhow::Result<Signer> {
    let trimmed = private_key.trim();
    let hex_body = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);

    if hex_body.len() == 64 && hex_body.chars().all(|c| c.is_ascii_hexdigit()) {
        let bytes = hex::decode(hex_body).context("invalid hex private key")?;
        let key_bytes: [u8; 32] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("hex private key must be 32 bytes"))?;
        return Signer::from_secret_key_bytes(&key_bytes)
            .map_err(|e| anyhow::anyhow!("invalid hex private key: {e}"));
    }

    Signer::from_secret_key_base64(trimmed)
        .map_err(|e| anyhow::anyhow!("invalid base64 private key: {e}"))
}
