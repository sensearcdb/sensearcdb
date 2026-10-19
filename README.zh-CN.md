# SenseArcDB

[English](README.md) | [简体中文](README.zh-CN.md)

**SenseArcDB** 是一个用 Rust 编写的工业 / 物联网时序数据库，定位为高性能、自包含的单机核心——对标传统实时数据库（如 PI System）与通用时序库（如 InfluxDB）的现代替代方案。

本仓库是**开源核心**，仅包含生产级的存储与查询引擎；企业级能力（AI 分析、告警、集群、安全合规、协议采集、边缘计算）位于商业版。

## 特性

- **列式存储（SAC）** — 自研磁盘格式，Gorilla / 位打包编码 + ZSTD9 压缩。
- **分级持久化 WAL** — group / durable / async 三档，基于序号去重的崩溃恢复。
- **实时快照** — 每个 tag 的最新值存储，支撑 `lastpoint` 查询。
- **LSM 合并** — 时间分区、按日对齐，最终合并为每分区单文件。
- **宽表模型** — `tag = (table_id, column_id)`；1 张表 = 1 个设备/实体，多列 = 多测点。
- **完整时序 SQL** — 多列投影（类型保真）、过滤、聚合 + `GROUP BY`、`SAMPLE BY` 时间桶、条件聚合（`FILTER`/`CASE`）、窗口函数（`lead`/`lag` `OVER`）。
- **连续查询 / 流计算 / 派生标签** — 降采样、流处理、计算标签。
- **基础多租户** — 逻辑租户隔离，按租户数据分离。
- **gRPC + REST** — 读走 gRPC Arrow IPC 流式，运维与简单写入走 REST。

## 性能

以下为单机内部实测数据（非与竞品的对比基准）——如需同口径对比，请在相同硬件上跑同一负载（如 TSBS）。

| 指标 | 数值 | 负载 |
|------|------|------|
| 端到端写入 | **110K rows/s** | TSBS IoT，10 列宽表（1 卡车 = 1 表），8 worker，gRPC |
| 引擎裸写 | **1.33M ops/s**（5.32M values/s） | columns = 4，引擎直调（无网络） |
| 引擎裸写 | **335K ops/s**（3.35M values/s） | columns = 10（TSBS 宽表） |
| 查询延迟 | **~0.75 ms P99** | 单表范围扫描 |

基准口径：`1 ops = 1 数据点 = 1 行 = --columns 个标量值`。

## 快速开始

```bash
cargo build --release
SENSEARCDB_CONFIG=config.toml ./target/release/sensearcdb
```

服务监听端口：

| 端口 | 协议 | 用途 |
|------|------|------|
| 5100 | HTTP (REST) | schema / 写入 / 查询 / 健康检查 |
| 5101 | HTTP | Prometheus 指标 |
| 5201 | gRPC | 写入（`WriteService.Ingest`） |
| 5202 | gRPC | 查询（`QueryService.Query`，Arrow IPC） |
| 5206 | gRPC | 快照 / 最新值（`BatchGetSnapshot`） |

> **查询说明**：历史查询走 gRPC `5202`（`QueryService.Query`，Arrow IPC）。REST 查询端点（`POST /api/v1/query`）尚未实现，返回空结果——请用 gRPC 查询。SQL 中表/列以 `table_<id>` / `col_<id>` 引用。

### 最小 `config.toml`

```toml
[paths]
data_dir = "./data"

[server]
rest_port = 5100
metrics_port = 5101
grpc_write_port = 5201
grpc_query_port = 5202
grpc_snapshot_port = 5206
```

> 开源核心**无认证**运行（透传可信的 `x-tenant-id`，默认租户 `0`）。TDE 与 mTLS **不在**开源构建中。见 [SECURITY.md](SECURITY.md)。

## 工作区布局

```
crates/
  sensearcdb-common      — 配置、类型化值、常量、错误
  sensearcdb-engine      — SAC、WAL、内存表、合并、加密、查询引擎
  sensearcdb-schema      — 目录、schema 镜像、租户管理
  sensearcdb-snapshot    — 快照存储、恢复、checkpointer
  sensearcdb-partition   — 时间分区
  sensearcdb-ingest      — gRPC/REST 读写服务（核心）
  sensearcdb-cq          — 连续查询
  sensearcdb-stream      — 流处理
  sensearcdb-derived     — 派生标签
  sensearcdb-bench       — 基准测试
  sensearcdb-observe     — 指标 / 追踪
  sensearcdb-deploy      — 启动引导、进程锁
  sensearcdb-server      — 单机二进制装配
```

## 开源版 vs 商业版

| 开源（本仓库） | 商业版（闭源） |
|----------------|----------------|
| 存储引擎、读写、宽表、基础多租户 | AI 分析（预测 / 异常 / 工况 / 资产）、告警 |
| | 多节点集群（Multi-Raft、分片、边缘） |
| | 安全合规（国密 SM2/SM3/SM4、mTLS、RBAC、审计、TDE） |
| | 协议采集（OPC-UA / Modbus / MQTT）、PI System 迁移 |

## 许可证

Apache 2.0 — 见 [LICENSE](LICENSE)。
