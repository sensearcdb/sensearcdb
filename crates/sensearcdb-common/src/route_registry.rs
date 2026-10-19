//! 挂载即登记路由注册表（S5 控制面鉴权专项修复 P2，D3；收尾①方法绑定）。
//!
//! 消除「手工 `MOUNTED_ENDPOINTS` 清单」与「`ROUTE_TABLE`」双真相源：所有进入主
//! `Router` 的路由统一走 [`RouteRegistryExt::register_route`]，在**同一次函数调用**里
//! 先登记真实 `(method, path)` 到运行时集合、再挂载。
//!
//! 收尾①：进一步把「HTTP 方法注解」与「method-router 构造器」绑定为**单一表达式**——
//! 调用方用 [`route!`] 宏书写 `get(h)` / `post(h)` / `put(h)` / `delete(h)` / `patch(h)`
//! 等构造器，方法名 `"GET"/"POST"/…` 由宏从**同一组 token** 自动派生，从结构上杜绝
//! 「注解方法 ≠ handler 方法」的错配（`register_route(&["GET"], path, post(h))` 这类
//! 写法不再存在）。`RouteSpec::explicit` 仅测试专用（验证 `register_route` 的显式标注
//! 登记路径），生产代码一律走 [`route!`] 宏。
//!
//! 控制面覆盖断言 [`control_plane_auth::verify_mounted_coverage`] 遍历该集合，从而
//! 让「真实挂载 ⊆ 路由表」成为单一真相源断言，能抓出方法错配 / 漏登 / 幽灵条目。

use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

use axum::routing::MethodRouter;
use axum::Router;

/// 运行时挂载端点集合（`(method, path)`，method 大写）。
static MOUNTED: OnceLock<Mutex<HashSet<(String, String)>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashSet<(String, String)>> {
    MOUNTED.get_or_init(|| Mutex::new(HashSet::new()))
}

/// 登记一条 `(method, path)`（method 统一大写，路径保持模板原样）。
pub fn record(method: &str, path: &str) {
    let mut guard = registry().lock().unwrap_or_else(|e| e.into_inner());
    guard.insert((method.to_ascii_uppercase(), path.to_string()));
}

/// 清空登记集合（`build_router` 开头调用，确保只反映本次构建的真实挂载）。
pub fn clear() {
    let mut guard = registry().lock().unwrap_or_else(|e| e.into_inner());
    guard.clear();
}

/// 取登记集合的快照（排序后返回，供覆盖断言遍历）。
pub fn snapshot() -> Vec<(String, String)> {
    let guard = registry().lock().unwrap_or_else(|e| e.into_inner());
    let mut out: Vec<(String, String)> = guard.iter().cloned().collect();
    out.sort();
    out
}

/// 「方法 + handler」绑定为单一表达式的产物：`methods` 与 `router` 同源。
///
/// 生产代码一律由 [`route!`] 宏构造（方法从 method 构造器 token 自动派生）；
/// `RouteSpec::explicit` 已收窄为测试专用（见其 `#[cfg(test)]` 门控），不再用于生产挂载。
pub struct RouteSpec<S> {
    /// 本路由覆盖的 HTTP 方法（大写/小写均可，登记时统一大写）。
    pub methods: &'static [&'static str],
    /// 与 `methods` 同源、实际挂载的 `MethodRouter`。
    pub router: MethodRouter<S>,
}

impl<S> RouteSpec<S> {
    /// 显式标注方法 + 预构建 `MethodRouter`（**测试专用**：验证 `register_route` 的显式
    /// 登记路径；生产代码一律走 [`route!`] 宏，方法无法静态派生的注入场景也改为注入
    /// handler 函数后再走宏）。
    #[cfg(test)]
    pub fn explicit(methods: &'static [&'static str], router: MethodRouter<S>) -> Self {
        Self { methods, router }
    }
}

/// 宏：把「HTTP 方法 + handler」绑定为单一表达式，方法名从构造器 token 自动派生。
///
/// 用法：`route!(get(h))`、`route!(get(a), post(b))`、`route!(put(a).delete(b))` 的等价
/// 形态 `route!(put(a), delete(b))`。展开为 [`RouteSpec`]（`methods` = 派生方法数组，
/// `router` = `axum::routing::get(...).post(...)` 链）。
///
/// 由此 `register_route(path, route!(get(h)))` 中的「方法注解」与「handler 方法」
/// 来自同一组 token，错配在结构上不可能发生。
#[macro_export]
macro_rules! route {
    ( $first:ident ( $($fh:tt)* ) $(, $rest:ident ( $($rh:tt)* ))* $(,)? ) => {
        $crate::route_registry::RouteSpec {
            methods: &[ stringify!($first) $(, stringify!($rest))* ],
            router: axum::routing::$first( $($fh)* ) $( .$rest( $($rh)* ) )*,
        }
    };
}

/// 「挂载即登记」扩展方法：替代 axum `Router::route`。
///
/// 接收一个 [`RouteSpec`]（由 [`route!`] 宏构造），内部先按 `spec.methods` 登记真实
/// `(method, path)`、再按 `spec.router` 挂载，保证登记与挂载同源。
pub trait RouteRegistryExt<S> {
    /// 登记 `spec.methods` × `path` 并挂载 `spec.router`，返回新的 `Router`。
    fn register_route(self, path: &str, spec: RouteSpec<S>) -> Router<S>;
}

impl<S> RouteRegistryExt<S> for Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    fn register_route(self, path: &str, spec: RouteSpec<S>) -> Router<S> {
        for method in spec.methods {
            record(method, path);
        }
        self.route(path, spec.router)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::{get, post, put};

    async fn h1() {}
    async fn h2() {}

    /// 收尾①结构保证：`route!` 宏的方法数组与 router 方法链同源（同一组 token 派生）。
    #[test]
    fn test_route_macro_derives_methods_from_constructors() {
        let spec: RouteSpec<()> = route!(get(h1));
        assert_eq!(spec.methods, &["get"]);

        let spec: RouteSpec<()> = route!(get(h1), post(h2));
        assert_eq!(spec.methods, &["get", "post"]);

        let spec: RouteSpec<()> = route!(get(h1), post(h2), put(h1));
        assert_eq!(spec.methods, &["get", "post", "put"]);
    }

    /// 收尾①：`register_route(path, route!(...))` 登记的方法与挂载方法同源、统一大写；
    /// `RouteSpec::explicit` 仅测试专用（验证显式标注登记路径）。
    ///
    /// 注：两者都读写全局 `MOUNTED` 集合，故合并为单个测试，避免并行测试互相 `clear` 竞态。
    #[test]
    fn test_register_route_records_derived_and_explicit_methods() {
        clear();
        let _router: Router<()> =
            Router::new().register_route("/api/v1/t", route!(get(h1), post(h2)));

        let snap = snapshot();
        assert_eq!(
            snap,
            vec![
                ("GET".to_string(), "/api/v1/t".to_string()),
                ("POST".to_string(), "/api/v1/t".to_string()),
            ]
        );

        // explicit：方法显式标注，登记结果与 route! 派生一致。
        let spec = RouteSpec::explicit(&["GET", "POST"], get(h1).post(h2));
        let _router2: Router<()> = Router::new().register_route("/api/v1/e", spec);
        let snap = snapshot();
        assert!(snap.contains(&("GET".to_string(), "/api/v1/e".to_string())));
        assert!(snap.contains(&("POST".to_string(), "/api/v1/e".to_string())));
        clear();
    }
}
