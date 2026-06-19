# Agent Instructions

This is a Rust workspace with two crates:

- `server` — core library
- `bin` — `orderbook_server` binary

Run these commands from the repository root after writing or modifying code:

```bash
cargo fmt
cargo clippy --workspace --all-targets
cargo test --workspace
```

### Formatting

- `cargo fmt` — apply formatting (uses `rustfmt.toml` at the repo root)
- `cargo fmt --check` — verify formatting without changing files

### Linting

- `cargo clippy --workspace --all-targets` — run Clippy on all crates and test targets
- `cargo check --workspace` — quick compile check without running tests

Workspace lints are configured in the root `Cargo.toml` under `[workspace.lints]`.

### Tests

- `cargo test --workspace` — run all unit and integration tests
- `cargo test -p server` — run tests for the library crate only
- `cargo test -p orderbook-server` — run tests for the binary crate only

Run the full sequence above before finishing a change. Fix any formatting, Clippy, or test failures before handing off.
