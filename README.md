# SenseArcDB

[English](README.md) | [简体中文](README.zh-CN.md)

**SenseArcDB** is an industrial & IoT time-series database written in Rust, designed as a high-performance, self-contained single-node core — a modern alternative to legacy historians (e.g. PI System) and general-purpose TSDBs (e.g. InfluxDB).

This repository contains the **open-source core**. It is production-grade storage and query engine only; enterprise capabilities (AI analytics, alerting, clustering, security/compliance, protocol collectors, edge) live in the commercial edition.

## Features

- **Columnar storage (SAC)** — custom on-disk format with Gorilla/bit-packed encoding and ZSTD9 compression.
- **WAL with tiered durability** — group / durable / async levels, crash recovery with seq-based dedup.
- **Real-time snapshot** — per-tag last-known-value store for `lastpoint` queries.
- **LSM compaction** — time-partitioned, day-aligned, with final compaction to a single file per partition.
- **Wide-table model** — `tag = (table_id, column_id)`; 1 table = 1 device/entity, multiple columns = multiple measurements.
- **Full time-series SQL** — multi-column projection with type fidelity, filtering, aggregation + `GROUP BY`, `SAMPLE BY` time buckets, conditional aggregation (`FILTER`/`CASE`), and window functions (`lead`/`lag` `OVER`).
- **Continuous queries / streams / derived tags** — downsampling, stream processing, and computed tags.
- **Foundational multi-tenancy** — logical tenant isolation with per-tenant data separation.
- **gRPC + REST** — Arrow IPC streaming over gRPC for reads, REST for ops and simple writes.

## Performance

Figures below are from internal measurements (single node). They are indicative, not a benchmark against competitors — for apples-to-apples comparison, run the same workload (e.g. TSBS) on identical hardware.

| Metric | Value | Workload |
|--------|-------|----------|
| Engine write | **1.33M rows/s** (5.32M values/s) | columns = 4, direct engine call, no network |
| Engine write | **335K rows/s** (3.35M values/s) | columns = 10 (TSBS wide table) |
| WAL throughput (group level) | **2.35M points/s** | page-cache write + background fsync |
| End-to-end ingest | 110K rows/s | TSBS IoT 10-col via gRPC + load toolchain (toolchain-bound, not engine-bound) |
| Query latency | ~0.75 ms P99 | single-table range scan |

Benchmark unit: `1 ops = 1 data point = 1 row = --columns scalar values`.

## Quick start

```bash
cargo build --release
SENSEARCDB_CONFIG=config.toml ./target/release/sensearcdb
```

The server listens on:

| Port | Protocol | Purpose |
|------|----------|---------|
| 5100 | HTTP (REST) | schema / write / query / health |
| 5101 | HTTP | Prometheus metrics |
| 5201 | gRPC | write (`WriteService.Ingest`) |
| 5202 | gRPC | query (`QueryService.Query`, Arrow IPC) |
| 5206 | gRPC | snapshot / lastpoint (`BatchGetSnapshot`) |

> **Querying**: historical queries run over gRPC `5202` (`QueryService.Query`, Arrow IPC). The REST query endpoint (`POST /api/v1/query`) is not yet implemented and returns empty results — use gRPC for queries. SQL references tables/columns as `table_<id>` / `col_<id>`.

### Minimal `config.toml`

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

> The open-source core runs **without authentication** (a trusted `x-tenant-id` is passed through, defaulting to tenant `0`). TDE and mTLS are **not** part of the open-source build. See [SECURITY.md](SECURITY.md).

## Workspace layout

```
crates/
  sensearcdb-common      — config, typed values, constants, errors
  sensearcdb-engine      — SAC, WAL, memtable, compaction, encryption, query engine
  sensearcdb-schema      — catalog, schema mirror, tenant manager
  sensearcdb-snapshot    — snapshot store, recovery, checkpointer
  sensearcdb-partition   — time partitioning
  sensearcdb-ingest      — gRPC/REST write & query services (core)
  sensearcdb-cq          — continuous queries
  sensearcdb-stream      — stream processing
  sensearcdb-derived     — derived tags
  sensearcdb-bench       — benchmarks
  sensearcdb-observe     — metrics / tracing
  sensearcdb-deploy      — bootstrap, process lock
  sensearcdb-server      — single-node binary assembly
```

## Open Core vs commercial

| Open source (this repo) | Commercial edition (closed) |
|-------------------------|-----------------------------|
| Storage engine, write/query, wide-table, foundational multi-tenancy | AI analytics (forecast / anomaly / condition / asset), alerting |
| | Multi-node clustering (Multi-Raft, sharding, edge) |
| | Security & compliance (国密 SM2/SM3/SM4, mTLS, RBAC, audit, TDE) |
| | Protocol collectors (OPC-UA / Modbus / MQTT), PI System migration |

## License

Apache 2.0 — see [LICENSE](LICENSE).
