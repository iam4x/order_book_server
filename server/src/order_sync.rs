use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use serde::{Deserialize, Serialize, de::IgnoredAny};
use tokio::sync::watch;

use crate::{
    prelude::Result,
    types::node_data::{Batch, NodeDataFill},
};

pub(crate) const ORDER_SYNC_PERIOD: Duration = Duration::from_secs(30);
const MAX_FILL_EVENTS_PER_BATCH: usize = 100_000;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct OrderSyncStatus {
    #[serde(rename = "last_order_at")]
    pub(crate) last_order_at: Option<u64>,
}

#[derive(Clone, Default)]
pub(crate) struct OrderSyncRecorder {
    latest_fill_time_ms: Arc<AtomicU64>,
}

impl OrderSyncRecorder {
    pub(crate) fn observe_batch(&self, batch: &Batch<NodeDataFill>) {
        if let Some(fill_time_ms) = batch.events_ref().iter().map(|NodeDataFill(_, fill)| fill.time).max() {
            self.observe_fill_time(fill_time_ms);
        }
    }

    pub(crate) fn observe_fill_line(&self, line: &str) -> Result<usize> {
        let batch: FillTimesBatch = sonic_rs::from_str(line)?;
        if batch.events.len() > MAX_FILL_EVENTS_PER_BATCH {
            return Err(format!(
                "fill batch contains {} events, above the {MAX_FILL_EVENTS_PER_BATCH} event limit",
                batch.events.len()
            )
            .into());
        }
        if let Some(fill_time_ms) = batch.events.iter().map(|(_, fill)| fill.time).max() {
            self.observe_fill_time(fill_time_ms);
        }
        Ok(batch.events.len())
    }

    fn observe_fill_time(&self, fill_time_ms: u64) {
        self.latest_fill_time_ms.fetch_max(fill_time_ms, Ordering::Relaxed);
    }

    pub(crate) fn status_at(&self, server_now_ms: u64) -> OrderSyncStatus {
        let latest_fill_time_ms = self.latest_fill_time_ms.load(Ordering::Relaxed);
        let last_order_at =
            (latest_fill_time_ms != 0).then(|| server_now_ms.saturating_sub(latest_fill_time_ms) / 1_000);
        OrderSyncStatus { last_order_at }
    }
}

pub(crate) struct OrderSyncHub {
    recorder: OrderSyncRecorder,
    updates: watch::Sender<OrderSyncStatus>,
}

impl OrderSyncHub {
    pub(crate) fn spawn() -> Arc<Self> {
        let recorder = OrderSyncRecorder::default();
        let (updates, initial_receiver) = watch::channel(recorder.status_at(0));
        drop(initial_receiver);
        let hub = Arc::new(Self { recorder, updates });
        tokio::spawn(Arc::clone(&hub).run());
        hub
    }

    pub(crate) fn recorder(&self) -> OrderSyncRecorder {
        self.recorder.clone()
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<OrderSyncStatus> {
        self.updates.subscribe()
    }

    async fn run(self: Arc<Self>) {
        let first_tick = tokio::time::Instant::now() + ORDER_SYNC_PERIOD;
        let mut ticker = tokio::time::interval_at(first_tick, ORDER_SYNC_PERIOD);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let server_now_ms = chrono::Utc::now().timestamp_millis().max(0).cast_unsigned();
            self.updates.send_replace(self.recorder.status_at(server_now_ms));
        }
    }
}

#[derive(Deserialize)]
struct FillTimesBatch {
    events: Vec<(IgnoredAny, FillTime)>,
}

#[derive(Deserialize)]
struct FillTime {
    time: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_tracks_null_age_future_and_out_of_order_fills() {
        let recorder = OrderSyncRecorder::default();
        assert_eq!(recorder.status_at(600_000).last_order_at, None);

        recorder.observe_fill_time(300_000);
        assert_eq!(recorder.status_at(600_000).last_order_at, Some(300));
        assert_eq!(recorder.status_at(299_999).last_order_at, Some(0));

        recorder.observe_fill_time(200_000);
        assert_eq!(recorder.status_at(600_000).last_order_at, Some(300));
    }

    #[test]
    fn partial_parser_uses_maximum_and_empty_batches_do_not_regress() {
        let recorder = OrderSyncRecorder::default();
        recorder
            .observe_fill_line(r#"{"events":[["ignored",{"time":500000}],[null,{"time":400000}]]}"#)
            .expect("fill times parse");
        assert_eq!(recorder.status_at(600_000).last_order_at, Some(100));

        recorder.observe_fill_line(r#"{"events":[]}"#).expect("empty fill batch parses");
        assert_eq!(recorder.status_at(600_000).last_order_at, Some(100));
    }

    #[tokio::test]
    async fn publisher_has_no_immediate_sample_and_uses_a_thirty_second_period() {
        assert_eq!(ORDER_SYNC_PERIOD, Duration::from_secs(30));
        let hub = OrderSyncHub::spawn();
        let receiver = hub.subscribe();
        tokio::task::yield_now().await;
        assert!(!receiver.has_changed().expect("publisher remains open"));
    }
}
