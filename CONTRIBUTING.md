# Contributing to SenseArcDB

Thanks for your interest in contributing!

## Workspace layout

SenseArcDB is a Cargo workspace of many crates. See the crate list in [README.md](README.md). The single-node binary lives in `crates/sensearcdb-server`.

## Dependency direction (hard rule)

The open-source core crates **must not** depend on any commercial (closed-source) crate. Dependencies flow one way only:

```
common ← engine / schema / snapshot / partition / ingest / cq / stream / derived / bench / observe / deploy ← server
```

When you add a new dependency to a core crate, make sure it does not pull in commercial crates (security / cluster / shard / edge / lifecycle / event / alert / forecast / condition / anomaly / ai-ops / asset / collector).

## Building & testing

```bash
cargo build --workspace
cargo test --workspace
```

The build needs an ASCII-only target directory if your checkout path contains non-ASCII characters:

```bash
export CARGO_TARGET_DIR="D:/sensearc-target-oss"   # Windows example
cargo build --workspace
```

## Pull requests

1. Fork the repo and create a feature branch.
2. Make focused changes; keep core-crate changes free of commercial dependencies.
3. Add or update tests for behavior changes.
4. Run `cargo test --workspace` locally before opening the PR.
5. Open a PR with a clear description of what and why.

## Commit conventions

- Use concise, imperative subject lines (e.g. `fix(query): preserve column types in projection`).
- Reference issues where relevant.
