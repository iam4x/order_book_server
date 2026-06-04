use std::{fmt, net::Ipv4Addr, path::PathBuf, str::FromStr};

use clap::Parser;
use server::{Result, ServerConfig, SnapshotMode, run_websocket_server};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Markets {
    include_perps: bool,
    include_spot: bool,
    include_hip3: bool,
}

impl Markets {
    const fn empty() -> Self {
        Self { include_perps: false, include_spot: false, include_hip3: false }
    }

    const fn flags(self) -> (bool, bool, bool) {
        (self.include_perps, self.include_spot, self.include_hip3)
    }
}

impl Default for Markets {
    fn default() -> Self {
        Self { include_perps: true, include_spot: true, include_hip3: true }
    }
}

impl FromStr for Markets {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() {
            return Err("empty market list; expected comma-separated values: perps, spot, hip3, or all".to_string());
        }

        if value == "all" {
            return Ok(Self::default());
        }

        let mut markets = Self::empty();
        for market in value.split(',') {
            let market = market.trim();
            if market.is_empty() {
                return Err(
                    "empty market entry; expected comma-separated values: perps, spot, hip3, or all".to_string()
                );
            }

            match market {
                "all" => {
                    return Err(
                        "`all` cannot be combined with specific markets; use `all` or `perps,spot,hip3`".to_string()
                    );
                }
                "perps" if !markets.include_perps => markets.include_perps = true,
                "perps" => return Err("duplicate market `perps`".to_string()),
                "spot" if !markets.include_spot => markets.include_spot = true,
                "spot" => return Err("duplicate market `spot`".to_string()),
                "hip3" if !markets.include_hip3 => markets.include_hip3 = true,
                "hip3" => return Err("duplicate market `hip3`".to_string()),
                unknown => {
                    return Err(format!(
                        "unknown market `{unknown}`; expected comma-separated values: perps, spot, hip3, or all"
                    ));
                }
            }
        }

        Ok(markets)
    }
}

impl fmt::Display for Markets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for (enabled, name) in [(self.include_perps, "perps"), (self.include_spot, "spot"), (self.include_hip3, "hip3")]
        {
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

#[derive(Debug, Parser)]
#[command(author, version, about = "Real-time Orderbook WebSocket Server for Hyperliquid")]
struct Args {
    /// Server address (e.g., 0.0.0.0)
    #[arg(long, default_value = "0.0.0.0")]
    address: Ipv4Addr,

    /// Server port (e.g., 8000)
    #[arg(long, default_value = "8000")]
    port: u16,

    /// Compression level for WebSocket connections (0-9).
    /// 0 = disabled, 1 = fastest (default), 9 = best ratio
    #[arg(long, default_value = "1")]
    compression_level: u32,

    /// Base directory for hlnode data files.
    /// For Docker: the directory containing .hyperliquid_rpc_hlnode_mainnet/
    /// For Direct: the directory containing hl/hyperliquid_data/
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Optional shared secret required as ?token=... on WebSocket connections
    #[arg(long)]
    secret: Option<String>,

    /// Which markets to include: comma-separated perps, spot, hip3, or all
    #[arg(long, default_value = "all")]
    markets: Markets,

    // ========== Snapshot Configuration ==========
    /// Snapshot fetching mode: docker or direct
    /// - docker: Use 'docker exec <container> hl-node ...' (for Docker users)
    /// - direct: Call 'hl-node ...' directly (for systemctl/bare metal users)
    #[arg(long, value_enum, default_value = "docker")]
    snapshot_mode: SnapshotMode,

    /// Docker container name (only used in docker mode)
    #[arg(long, default_value = "hyperliquid_hlnode")]
    docker_container: String,

    /// Path to hl-node binary (only used in direct mode).
    /// Default: 'hl-node' (assumes in PATH)
    #[arg(long, default_value = "hl-node")]
    hlnode_binary: String,

    /// Path to abci_state.rmp file (only used in direct mode).
    /// Default: <data_dir>/hl/hyperliquid_data/abci_state.rmp
    #[arg(long)]
    abci_state_path: Option<PathBuf>,

    /// Path where snapshot.json will be written (only used in direct mode).
    /// Default: /tmp/hl_snapshot.json
    #[arg(long)]
    snapshot_output_path: Option<PathBuf>,

    /// Path to visor_abci_state.json (optional, for height info).
    /// Default: <data_dir>/.hyperliquid_rpc_hlnode_mainnet/volumes/hl/hyperliquid_data/visor_abci_state.json
    #[arg(long)]
    visor_state_path: Option<PathBuf>,

    /// Port for Prometheus metrics endpoint (0 to disable)
    #[arg(long, default_value = "9090")]
    metrics_port: u16,

    /// BBO-only mode: lightweight mode that only tracks best bid/ask per coin.
    /// Reduces RAM from 2-3GB to ~100MB. Disables L2/L4/Trades subscriptions.
    #[arg(long, default_value = "false")]
    bbo_only: bool,

    /// Resend the last l2Book snapshot for each active subscription every N ms
    /// when nothing has changed. Off by default (0 = disabled). Matches the
    /// official Hyperliquid API behavior of pushing a heartbeat snapshot per block
    /// so downstream clients with stall timers don't disconnect on quiet coins.
    #[arg(long, default_value = "0")]
    l2book_heartbeat_ms: u64,

    /// Resend the last bbo payload for each active subscription every N ms
    /// when nothing has changed. Off by default (0 = disabled).
    #[arg(long, default_value = "0")]
    bbo_heartbeat_ms: u64,

    /// Log level: error, warn, info, debug, trace
    #[arg(long, default_value = "info")]
    log_level: String,
}

/// Start the Prometheus metrics HTTP server
async fn start_metrics_server(port: u16) {
    use axum::{Router, response::IntoResponse, routing::get};

    async fn metrics_handler() -> impl IntoResponse {
        server::metrics::gather_metrics()
    }

    let app = Router::new().route("/metrics", get(metrics_handler));
    let addr = format!("0.0.0.0:{}", port);

    log::info!("Metrics server listening on http://{}/metrics", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.expect("failed to bind metrics port");
    axum::serve(listener, app).await.expect("metrics server failed");
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logger with specified level
    // SAFETY: We're setting this before any threads are spawned
    #[allow(unsafe_code)]
    unsafe {
        std::env::set_var("RUST_LOG", &args.log_level);
    }
    env_logger::init();

    // Register Prometheus metrics
    server::metrics::register_metrics();

    let full_address = format!("{}:{}", args.address, args.port);

    let (include_perps, include_spot, include_hip3) = args.markets.flags();

    // Build config
    let config = ServerConfig {
        address: full_address.clone(),
        compression_level: args.compression_level,
        data_dir: args.data_dir,
        secret: args.secret,
        include_perps,
        include_spot,
        include_hip3,
        snapshot_mode: args.snapshot_mode,
        docker_container: args.docker_container,
        hlnode_binary: args.hlnode_binary,
        abci_state_path: args.abci_state_path,
        snapshot_output_path: args.snapshot_output_path,
        visor_state_path: args.visor_state_path,
        metrics_port: args.metrics_port,
        bbo_only: args.bbo_only,
        l2book_heartbeat_ms: args.l2book_heartbeat_ms,
        bbo_heartbeat_ms: args.bbo_heartbeat_ms,
    };

    println!("Orderbook Server v{}", env!("CARGO_PKG_VERSION"));
    println!("  Address: {}", config.address);
    println!("  Markets: {}", args.markets);
    if config.bbo_only {
        println!("  Mode: BBO-ONLY (lightweight, ~100MB RAM)");
        println!("  Note: L2/L4/Trades subscriptions disabled");
    }
    println!("  Snapshot mode: {:?}", config.snapshot_mode);
    match config.snapshot_mode {
        SnapshotMode::Docker => {
            println!("  Container: {}", config.docker_container);
        }
        SnapshotMode::Direct => {
            println!("  hl-node binary: {}", config.hlnode_binary);
            if let Some(ref path) = config.abci_state_path {
                println!("  abci_state: {}", path.display());
            }
            if let Some(ref path) = config.snapshot_output_path {
                println!("  snapshot output: {}", path.display());
            }
        }
    }
    if let Some(ref dir) = config.data_dir {
        println!("  Data dir: {}", dir.display());
    }
    if config.metrics_port > 0 {
        println!("  Metrics: http://0.0.0.0:{}/metrics", config.metrics_port);
    }
    println!("  WebSocket auth: {}", if config.secret.is_some() { "enabled" } else { "disabled" });
    println!("  Log level: {}", args.log_level);
    println!();

    // Spawn uptime counter
    tokio::spawn(async {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            server::metrics::UPTIME_SECONDS.inc();
        }
    });

    // Start metrics server if port > 0
    if config.metrics_port > 0 {
        let metrics_port = config.metrics_port;
        tokio::spawn(async move {
            start_metrics_server(metrics_port).await;
        });
    }

    tokio::select! {
        result = run_websocket_server(config) => {
            if let Err(e) = result {
                log::error!("Server error: {e}");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            log::info!("Shutdown signal received, exiting gracefully");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser as _;

    use super::{Args, Markets};

    const ALL_MARKETS: Markets = Markets { include_perps: true, include_spot: true, include_hip3: true };

    const PERPS_ONLY: Markets = Markets { include_perps: true, include_spot: false, include_hip3: false };

    const SPOT_ONLY: Markets = Markets { include_perps: false, include_spot: true, include_hip3: false };

    const HIP3_ONLY: Markets = Markets { include_perps: false, include_spot: false, include_hip3: true };

    #[test]
    fn parses_single_market_values() {
        assert_eq!("perps".parse::<Markets>(), Ok(PERPS_ONLY));
        assert_eq!("spot".parse::<Markets>(), Ok(SPOT_ONLY));
        assert_eq!("hip3".parse::<Markets>(), Ok(HIP3_ONLY));
    }

    #[test]
    fn parses_comma_delimited_market_values() {
        assert_eq!(
            "perps,hip3".parse::<Markets>(),
            Ok(Markets { include_perps: true, include_spot: false, include_hip3: true })
        );
        assert_eq!("perps,spot,hip3".parse::<Markets>(), Ok(ALL_MARKETS));
    }

    #[test]
    fn parses_all_as_compatibility_alias() {
        assert_eq!("all".parse::<Markets>(), Ok(ALL_MARKETS));
    }

    #[test]
    fn trims_whitespace_around_market_values() {
        assert_eq!(
            " perps, hip3 ".parse::<Markets>(),
            Ok(Markets { include_perps: true, include_spot: false, include_hip3: true })
        );
    }

    #[test]
    fn rejects_invalid_market_values() {
        assert!("foo".parse::<Markets>().is_err());
        assert!("perps,,hip3".parse::<Markets>().is_err());
        assert!("perps,perps".parse::<Markets>().is_err());
        assert!("all,hip3".parse::<Markets>().is_err());
    }

    #[test]
    fn cli_parses_comma_delimited_markets() {
        let args = Args::try_parse_from(["orderbook_server", "--markets", "perps,hip3"])
            .expect("comma-delimited markets should parse");

        assert_eq!(args.markets, Markets { include_perps: true, include_spot: false, include_hip3: true });
    }

    #[test]
    fn cli_secret_is_optional() {
        let args = Args::try_parse_from(["orderbook_server"]).expect("default args should parse");

        assert_eq!(args.secret, None);
    }

    #[test]
    fn cli_parses_secret() {
        let args = Args::try_parse_from(["orderbook_server", "--secret", "super-secret"]).expect("secret should parse");

        assert_eq!(args.secret.as_deref(), Some("super-secret"));
    }
}
