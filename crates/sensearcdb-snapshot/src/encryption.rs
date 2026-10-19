//! snapshot 侧 TDE 加密抽象。
//!
//! `sensearcdb-snapshot` 不依赖 `sensearcdb-engine`，故在此定义与 engine
//! `Encryptor` 语义一致的 `SnapshotEncryptor` trait，由启动层（`src/tde_adapter.rs`）
//! 用 engine 的 `Encryptor` 实现后注入 `recover` / `serialize_to_file`。
//!
//! TDE 关闭时传入 `None`（`SharedSnapshotEncryptor`），行为与明文完全一致。

use std::sync::Arc;

use sensearcdb_common::SenseArcError;

/// snapshot.bin 加密错误（snapshot crate 自包含，不引入 engine 的错误类型）。
#[derive(Debug, thiserror::Error)]
pub enum SnapshotEncryptError {
    /// 底层加密运算失败。
    #[error("crypto operation failed: {0}")]
    Crypto(String),
    /// 检测到加密快照但没有提供 Encryptor（缺少 TDE 密钥）。
    #[error("encrypted snapshot detected but no Encryptor provided (missing TDE key)")]
    NoEncryptor,
    /// 未知的 DEK 版本。
    #[error("unknown DEK version {0}")]
    UnknownDek(u32),
    /// 密文 blob 过短。
    #[error("ciphertext blob too short: {0} < {1}")]
    TooShort(usize, usize),
    /// GCM 认证失败（数据被篡改或密钥错误）。
    #[error("GCM authentication failed (tampered data or wrong key)")]
    AuthFailed,
}

/// snapshot 加密错误 → 统一错误收敛（TDE 错误）。
impl From<SnapshotEncryptError> for SenseArcError {
    fn from(e: SnapshotEncryptError) -> Self {
        SenseArcError::TdeError(e.to_string())
    }
}

/// snapshot 侧加密抽象（语义与 engine 的 `Encryptor` 一致，但避免跨 crate 依赖）。
pub trait SnapshotEncryptor: Send + Sync {
    /// 用活跃 DEK 加密明文，返回自包含密文：`nonce(12B) || tag(16B) || ct`。
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, SnapshotEncryptError>;

    /// 按 DEK 版本解密自包含密文，返回明文。
    fn decrypt(&self, blob: &[u8], dek_version: u32) -> Result<Vec<u8>, SnapshotEncryptError>;

    /// 当前活跃 DEK 版本（写盘时写入文件头，供解密定位密钥）。
    fn active_dek_version(&self) -> u32;

    /// 是否启用 TDE。
    fn is_enabled(&self) -> bool;
}

/// 便捷类型：启动层统一持有的可选 snapshot 加密器。
pub type SharedSnapshotEncryptor = Option<Arc<dyn SnapshotEncryptor>>;
