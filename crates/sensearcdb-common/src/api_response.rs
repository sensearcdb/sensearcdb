//! 统一 REST 响应 envelope。
//!
//! 成功：`{ "code": 0, "message": "ok", "data": <payload> }`
//! 失败：`{ "code": <5 位错误码>, "message": <描述>, "data": null }`
//!
//! 错误码与 [`SenseArcError::code()`](crate::SenseArcError::code) 一致（阶段 0 锁死）。

use serde::Serialize;

/// 统一 API 响应 envelope。
#[derive(Debug, Clone, Serialize)]
pub struct ApiResponse<T> {
    /// 业务码：`0` 表示成功；非 `0` 为 5 位错误码。
    pub code: u32,
    /// 消息：成功恒为 `"ok"`，失败为错误描述。
    pub message: String,
    /// 数据负载：失败时为 `None`（序列化为 JSON `null`）。
    pub data: Option<T>,
}

impl<T: Serialize> ApiResponse<T> {
    /// 构造成功响应。
    pub fn ok(data: T) -> Self {
        Self {
            code: 0,
            message: "ok".to_string(),
            data: Some(data),
        }
    }

    /// 构造失败响应。
    pub fn err(code: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ok_envelope() {
        let resp = ApiResponse::ok(42u32);
        assert_eq!(resp.code, 0);
        assert_eq!(resp.message, "ok");
        assert_eq!(resp.data, Some(42));
    }

    #[test]
    fn test_err_envelope() {
        let resp = ApiResponse::<u32>::err(81001, "model load failed");
        assert_eq!(resp.code, 81001);
        assert_eq!(resp.message, "model load failed");
        assert_eq!(resp.data, None);
    }

    #[test]
    fn test_serialization_shape() {
        let ok = serde_json::to_value(ApiResponse::ok(7u32)).unwrap();
        assert_eq!(ok["code"], 0);
        assert_eq!(ok["message"], "ok");
        assert_eq!(ok["data"], 7);

        let err = serde_json::to_value(ApiResponse::<u32>::err(83001, "not init")).unwrap();
        assert_eq!(err["code"], 83001);
        assert!(err["data"].is_null());
    }
}
