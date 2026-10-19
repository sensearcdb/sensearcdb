//! engine 侧 TDE 加密抽象（依赖倒置：engine 不依赖 security crate）。
//!
//! `Encryptor` trait 定义在消费者侧（engine），由启动层用 security 的
//! `TdeEngine` 实现后注入 `Engine::new`。TDE 关闭时传入 `None`（`SharedEncryptor`）。
//!
//! ## 密文 blob 线上格式（engine 与 adapter 共同遵守）
//!
//! ```text
//! +---------+---------+---------------------+
//! | nonce   | tag     | ciphertext          |
//! | 12B     | 16B     | 变长（= 明文长度）    |
//! +---------+---------+---------------------+
//! ```
//!
//! GCM 密文长度 = 明文长度，故 `ciphertext.len() == plaintext.len()`，
//! reader 解压无需额外长度字段。

use std::sync::Arc;

use sensearcdb_common::{SenseArcError, TenantId};

/// GCM nonce 长度（96-bit）。
pub const NONCE_LEN: usize = 12;
/// GCM 认证标签长度（128-bit）。
pub const GCM_TAG_LEN: usize = 16;
/// 自包含密文 blob 固定开销：nonce(12B) + tag(16B)。
pub const ENCRYPTED_BLOB_OVERHEAD: usize = NONCE_LEN + GCM_TAG_LEN; // = 28

/// 加密错误（engine 侧自包含，不引入 security 的 String 错误）。
#[derive(Debug, thiserror::Error)]
pub enum EncryptError {
    /// 底层加密运算失败。
    #[error("crypto operation failed: {0}")]
    Crypto(String),
    /// 检测到密文但没有提供 Encryptor（缺少 TDE 密钥）。
    #[error("encrypted data detected but no Encryptor provided (missing TDE key)")]
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

/// 将加密错误转换为统一引擎错误（engine 依赖 common，可在此处实现 `From`）。
impl From<EncryptError> for SenseArcError {
    fn from(e: EncryptError) -> Self {
        SenseArcError::TdeError(e.to_string())
    }
}

/// 加密上下文：定位密文归属（租户）+ 指定解密密钥版本。
#[derive(Debug, Clone)]
pub struct EncryptContext {
    /// 租户 ID（加密域归属，`(tenant_id, dek_version)` 定位 DEK）。
    pub tenant_id: TenantId,
    /// DEK 版本号：0 = 明文/未加密，非 0 = 用该租户该版本 DEK。
    pub dek_version: u32,
    /// GCM 附加认证数据（绑定密文到其物理位置）。阶段 1 传空，P2 启用。
    pub aad: Vec<u8>,
}

impl EncryptContext {
    /// 构造 SAC 流上下文。
    ///
    /// O-4（已裁决延后）：AAD 阶段 1 恒空，待 `sm4-gcm` crate 支持 AAD 入参后启用
    /// `"SAC" || file_id || block_id || stream_id` 绑定；当前仅保留入参供后续填充。
    pub fn sac_stream(
        tenant_id: TenantId,
        file_id: u64,
        block_id: u64,
        stream_id: u32,
        dek_version: u32,
    ) -> Self {
        // O-4: aad 留空。
        let _ = (file_id, block_id, stream_id);
        Self {
            tenant_id,
            dek_version,
            aad: Vec::new(),
        }
    }

    /// 构造 WAL 帧上下文。
    ///
    /// O-4（已裁决延后）：AAD 阶段 1 恒空，待 `sm4-gcm` crate 支持 AAD 入参后启用
    /// `"WAL" || segment_seq || frame_offset` 绑定。
    pub fn wal_frame(
        tenant_id: TenantId,
        segment_seq: u64,
        frame_offset: u64,
        dek_version: u32,
    ) -> Self {
        // O-4: aad 留空。
        let _ = (segment_seq, frame_offset);
        Self {
            tenant_id,
            dek_version,
            aad: Vec::new(),
        }
    }
}

/// engine 侧加密抽象。
///
/// 由启动层用 security 的 `TdeEngine` 实现本 trait 后注入 `Engine::new`。
/// TDE 关闭时传入 `None`。
pub trait Encryptor: Send + Sync {
    /// 加密明文，返回自包含密文：`nonce(12B) || tag(16B) || ciphertext`。
    fn encrypt(&self, plaintext: &[u8], ctx: &EncryptContext) -> Result<Vec<u8>, EncryptError>;

    /// 解密自包含密文，返回明文（长度 = 原 LZ4 压缩流长度）。
    fn decrypt(&self, blob: &[u8], ctx: &EncryptContext) -> Result<Vec<u8>, EncryptError>;

    /// 某租户当前活跃 DEK 版本（写盘时写入文件头，供解密时定位密钥）。
    fn active_dek_version(&self, tenant_id: TenantId) -> u32;

    /// 是否启用 TDE。
    fn is_enabled(&self) -> bool;
}

/// 便捷类型：Engine/SacWriter/WalWriter 统一持有的可选加密器。
pub type SharedEncryptor = Option<Arc<dyn Encryptor>>;

/// 加密器存在且启用时加密，否则原样返回（明文路径，TDE 关闭）。
///
/// # 参数
/// - `encryptor`: 可选加密器引用（`None` = TDE 关）。
/// - `plaintext`: 已压缩的明文字节。
/// - `ctx`: 加密上下文（定位密文归属）。
pub fn encrypt_with(
    encryptor: Option<&dyn Encryptor>,
    plaintext: &[u8],
    ctx: &EncryptContext,
) -> Result<Vec<u8>, EncryptError> {
    match encryptor {
        Some(e) if e.is_enabled() => e.encrypt(plaintext, ctx),
        _ => Ok(plaintext.to_vec()),
    }
}

/// 依据文件内「已加密」标志解密；明文路径原样返回，密文无密钥则硬报错。
///
/// # 参数
/// - `encryptor`: 可选加密器引用。
/// - `encrypted`: 该流是否已被加密（由文件头/帧前缀自描述）。
/// - `blob`: 存储的流字节（明文压缩流或 `nonce||tag||ct`）。
/// - `ctx`: 解密上下文（含 dek_version）。
pub fn decrypt_with(
    encryptor: Option<&dyn Encryptor>,
    encrypted: bool,
    blob: &[u8],
    ctx: &EncryptContext,
) -> Result<Vec<u8>, EncryptError> {
    if encrypted {
        match encryptor {
            Some(e) => e.decrypt(blob, ctx),
            None => Err(EncryptError::NoEncryptor),
        }
    } else {
        Ok(blob.to_vec())
    }
}
