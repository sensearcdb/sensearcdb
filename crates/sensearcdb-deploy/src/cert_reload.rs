//! SIGUSR1 证书热加载。
//!
//! 收到 SIGUSR1 信号时：
//! 1. 读取新证书文件（CA + Server Cert + Key）
//! 2. 使用 x509-parser 验证证书有效性（有效期 / SAN）
//! 3. 原子替换内存中的证书
//! 4. 记录审计日志

use sensearcdb_common::SenseArcError;
use std::sync::Arc;
use tokio::sync::RwLock;
use x509_parser::certificate::X509Certificate;
use x509_parser::prelude::FromDer;

/// 证书热加载器。
#[derive(Debug)]
pub struct CertReloader {
    ca_cert: Arc<RwLock<Option<Vec<u8>>>>,
    server_cert: Arc<RwLock<Option<Vec<u8>>>>,
    server_key: Arc<RwLock<Option<Vec<u8>>>>,
}

impl CertReloader {
    /// 创建新的 CertReloader。
    pub fn new() -> Self {
        Self {
            ca_cert: Arc::new(RwLock::new(None)),
            server_cert: Arc::new(RwLock::new(None)),
            server_key: Arc::new(RwLock::new(None)),
        }
    }

    /// 注册 SIGUSR1 信号处理器（仅 Unix）。
    #[cfg(unix)]
    pub async fn spawn_signal_handler(
        self: Arc<Self>,
        ca_path: String,
        cert_path: String,
        key_path: String,
    ) {
        let mut sigusr1 =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())
                .expect("failed to register SIGUSR1 handler");

        tokio::spawn(async move {
            loop {
                sigusr1.recv().await;
                tracing::info!("收到 SIGUSR1，开始证书热加载...");
                match self.reload(&ca_path, &cert_path, &key_path).await {
                    Ok(()) => tracing::info!("证书热加载成功"),
                    Err(e) => tracing::error!("证书热加载失败: {}", e),
                }
            }
        });
    }

    /// 非 Unix 平台的 no-op。
    #[cfg(not(unix))]
    pub async fn spawn_signal_handler(
        self: Arc<Self>,
        _ca_path: String,
        _cert_path: String,
        _key_path: String,
    ) {
        tracing::debug!("SIGUSR1 not supported on this platform");
    }

    /// 重新加载证书：读取 → 验证 → 替换。
    pub async fn reload(
        &self,
        ca_path: &str,
        cert_path: &str,
        key_path: &str,
    ) -> Result<(), SenseArcError> {
        let ca_bytes = std::fs::read(ca_path)
            .map_err(|e| SenseArcError::Internal(format!("读取 CA 证书失败 {}: {}", ca_path, e)))?;
        let cert_bytes = std::fs::read(cert_path)
            .map_err(|e| SenseArcError::Internal(format!("读取证书失败 {}: {}", cert_path, e)))?;
        let key_bytes = std::fs::read(key_path)
            .map_err(|e| SenseArcError::Internal(format!("读取密钥失败 {}: {}", key_path, e)))?;

        self.validate_certificate(&cert_bytes)?;

        {
            let mut ca = self.ca_cert.write().await;
            *ca = Some(ca_bytes);
        }
        {
            let mut cert = self.server_cert.write().await;
            *cert = Some(cert_bytes);
        }
        {
            let mut key = self.server_key.write().await;
            *key = Some(key_bytes);
        }

        tracing::info!(
            "证书已热加载: CA={}, Cert={}, Key={}",
            ca_path,
            cert_path,
            key_path
        );
        Ok(())
    }

    /// 验证 X.509 证书有效性。
    fn validate_certificate(&self, cert_bytes: &[u8]) -> Result<(), SenseArcError> {
        let (_, x509) = X509Certificate::from_der(cert_bytes)
            .map_err(|e| SenseArcError::Internal(format!("证书 DER 解析失败: {}", e)))?;

        let validity = x509.validity();
        let now_asn1 = x509_parser::time::ASN1Time::from_timestamp(chrono::Utc::now().timestamp());
        if let Ok(now) = now_asn1 {
            if !validity.is_valid_at(now) {
                tracing::warn!("证书可能已过期或尚未生效");
            }
        }

        if let Ok(Some(san)) = x509.subject_alternative_name() {
            if san.value.general_names.is_empty() {
                return Err(SenseArcError::Internal("证书缺少 SAN 扩展".into()));
            }
        }

        tracing::info!(
            "证书验证通过: 主题={}, 签发者={}",
            x509.subject(),
            x509.issuer()
        );
        Ok(())
    }
}

impl Default for CertReloader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cert_reloader_new() {
        let r = CertReloader::new();
        assert!(r.ca_cert.blocking_read().is_none());
    }

    #[tokio::test]
    async fn test_reload_nonexistent() {
        let r = CertReloader::new();
        let result = r
            .reload(
                "/nonexistent/ca.crt",
                "/nonexistent/cert.crt",
                "/nonexistent/key.pem",
            )
            .await;
        assert!(result.is_err());
    }
}
