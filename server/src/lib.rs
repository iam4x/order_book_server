#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
mod listeners;
pub mod metrics;
mod order_book;
mod prelude;
mod servers;
mod types;

use std::{fmt, path::PathBuf, str::FromStr};

use clap::ValueEnum;
pub use prelude::Result;
pub use servers::websocket_server::run_websocket_server;

/// Snapshot fetching mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum SnapshotMode {
    /// Use docker exec to call hl-node inside container
    #[default]
    Docker,
    /// Call hl-node directly (for systemctl/bare metal setups)
    Direct,
}

/// Optional WebSocket channels and the upstream work needed to serve them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeatureSet {
    bbo: bool,
    allbbo: bool,
    l2book: bool,
    l4book: bool,
    trades: bool,
    bookdiffs: bool,
    orderupdates: bool,
}

impl FeatureSet {
    pub const fn all() -> Self {
        Self { bbo: true, allbbo: true, l2book: true, l4book: true, trades: true, bookdiffs: true, orderupdates: true }
    }

    const fn empty() -> Self {
        Self {
            bbo: false,
            allbbo: false,
            l2book: false,
            l4book: false,
            trades: false,
            bookdiffs: false,
            orderupdates: false,
        }
    }

    pub const fn bbo(self) -> bool {
        self.bbo
    }

    pub const fn allbbo(self) -> bool {
        self.allbbo
    }

    pub const fn l2book(self) -> bool {
        self.l2book
    }

    pub const fn l4book(self) -> bool {
        self.l4book
    }

    pub const fn trades(self) -> bool {
        self.trades
    }

    pub const fn bookdiffs(self) -> bool {
        self.bookdiffs
    }

    pub const fn orderupdates(self) -> bool {
        self.orderupdates
    }

    pub const fn requires_book_state(self) -> bool {
        self.bbo || self.allbbo || self.l2book || self.l4book
    }

    pub const fn watch_order_statuses(self) -> bool {
        self.requires_book_state() || self.orderupdates
    }

    pub const fn watch_order_diffs(self) -> bool {
        self.requires_book_state() || self.bookdiffs
    }

    pub const fn watch_fills(self) -> bool {
        self.trades
    }
}

impl Default for FeatureSet {
    fn default() -> Self {
        Self::all()
    }
}

impl FromStr for FeatureSet {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() {
            return Err(
                "empty feature list; expected comma-separated values: bbo, allbbo, l2book, l4book, trades, bookdiffs, orderupdates, or all"
                    .to_string(),
            );
        }

        if value == "all" {
            return Ok(Self::all());
        }

        let mut features = Self::empty();
        for feature in value.split(',') {
            let feature = feature.trim();
            if feature.is_empty() {
                return Err(
                    "empty feature entry; expected comma-separated values: bbo, allbbo, l2book, l4book, trades, bookdiffs, orderupdates, or all"
                        .to_string(),
                );
            }

            match feature {
                "all" => {
                    return Err(
                        "`all` cannot be combined with specific features; use `all` or an explicit comma-separated feature list"
                            .to_string(),
                    );
                }
                "bbo" if !features.bbo => features.bbo = true,
                "bbo" => return Err("duplicate feature `bbo`".to_string()),
                "allbbo" if !features.allbbo => features.allbbo = true,
                "allbbo" => return Err("duplicate feature `allbbo`".to_string()),
                "l2book" if !features.l2book => features.l2book = true,
                "l2book" => return Err("duplicate feature `l2book`".to_string()),
                "l4book" if !features.l4book => features.l4book = true,
                "l4book" => return Err("duplicate feature `l4book`".to_string()),
                "trades" if !features.trades => features.trades = true,
                "trades" => return Err("duplicate feature `trades`".to_string()),
                "bookdiffs" if !features.bookdiffs => features.bookdiffs = true,
                "bookdiffs" => return Err("duplicate feature `bookdiffs`".to_string()),
                "orderupdates" if !features.orderupdates => features.orderupdates = true,
                "orderupdates" => return Err("duplicate feature `orderupdates`".to_string()),
                unknown => {
                    return Err(format!(
                        "unknown feature `{unknown}`; expected comma-separated values: bbo, allbbo, l2book, l4book, trades, bookdiffs, orderupdates, or all"
                    ));
                }
            }
        }

        Ok(features)
    }
}

impl fmt::Display for FeatureSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for (enabled, name) in [
            (self.bbo, "bbo"),
            (self.allbbo, "allbbo"),
            (self.l2book, "l2book"),
            (self.l4book, "l4book"),
            (self.trades, "trades"),
            (self.bookdiffs, "bookdiffs"),
            (self.orderupdates, "orderupdates"),
        ] {
            if enabled {
                if !first {
                    write!(f, ",")?;
                }
                write!(f, "{name}")?;
                first = false;
            }
        }
        Ok(())
    }
}

/// Server configuration passed from CLI arguments
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Full address string (e.g., "0.0.0.0:8000")
    pub address: String,
    /// WebSocket compression level (0-9)
    pub compression_level: u32,
    /// Optional base directory for hlnode data
    pub data_dir: Option<PathBuf>,
    /// Optional shared secret required as ?token=... on WebSocket handshakes
    pub secret: Option<String>,
    /// Include perpetual futures markets
    pub include_perps: bool,
    /// Include spot markets (@ coins, PURR/USDC)
    pub include_spot: bool,
    /// Include HIP-3 markets
    pub include_hip3: bool,
    /// Snapshot fetching mode (docker or direct)
    pub snapshot_mode: SnapshotMode,
    /// Docker container name for exec commands (docker mode only)
    pub docker_container: String,
    /// Path to hl-node binary (direct mode only)
    pub hlnode_binary: String,
    /// Path to abci_state.rmp file (direct mode only, has default)
    pub abci_state_path: Option<PathBuf>,
    /// Path where snapshot will be written (direct mode only, has default)
    pub snapshot_output_path: Option<PathBuf>,
    /// Path to visor_abci_state.json (optional)
    pub visor_state_path: Option<PathBuf>,
    /// Port for Prometheus metrics endpoint (0 to disable)
    pub metrics_port: u16,
    /// Enabled WebSocket features and the upstream processing required by them
    pub features: FeatureSet,
    /// Resend the last l2Book payload every N ms when nothing has changed.
    /// 0 = disabled (default). Provides a heartbeat for low-liquidity coins
    /// whose snapshot hash rarely changes, matching the official HL API behavior.
    pub l2book_heartbeat_ms: u64,
    /// Resend the last bbo payload every N ms when nothing has changed.
    /// 0 = disabled (default).
    pub bbo_heartbeat_ms: u64,
    /// Resend the last allbbo payload every N ms when nothing has changed.
    /// 0 = disabled (default).
    pub allbbo_heartbeat_ms: u64,
}
