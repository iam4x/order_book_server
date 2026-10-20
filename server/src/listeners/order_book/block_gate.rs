use tokio::time::{Duration, Instant};

/// hl-node writes a block's lines back to back, so book streams that stay at EOF
/// this long mean the open block has no more lines coming.
pub(super) const BLOCK_QUIET: Duration = Duration::from_millis(2);

/// Tracks whether the book holds whole blocks, so book-derived feeds never
/// publish a block that is only partly applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BlockGate {
    /// The book holds every line of the blocks applied so far; book-derived feeds may publish.
    Complete { block: u64 },
    /// Lines of `block` are still arriving. `idle_since` is set once every book stream hit EOF.
    Open { block: u64, idle_since: Option<Instant> },
}

impl Default for BlockGate {
    fn default() -> Self {
        Self::Complete { block: 0 }
    }
}

impl BlockGate {
    pub(super) const fn open(block: u64) -> Self {
        Self::Open { block, idle_since: None }
    }

    pub(super) const fn idle(block: u64, since: Instant) -> Self {
        Self::Open { block, idle_since: Some(since) }
    }

    const fn block(self) -> u64 {
        match self {
            Self::Complete { block } | Self::Open { block, .. } => block,
        }
    }

    /// Call before applying an order-diff line. Returns true when the line closed the open block,
    /// leaving the gate complete so the caller can publish it before opening `block`.
    pub(super) const fn close_before_diff(&mut self, block: u64) -> bool {
        // Any other block closes it, not only a higher one: hl-node restarts rewrite older blocks.
        let Self::Open { block: open, .. } = *self else { return false };
        if open == block {
            return false;
        }
        *self = Self::Complete { block: open };
        true
    }

    /// Status lines can run blocks ahead of diffs, so they keep the diff block open without moving it.
    pub(super) const fn on_status_line(&mut self) {
        *self = Self::open(self.block());
    }

    pub(super) const fn on_books_idle(&mut self, now: Instant) {
        if let Self::Open { idle_since: idle_since @ None, .. } = self {
            *idle_since = Some(now);
        }
    }

    pub(super) fn quiet_deadline(self) -> Option<Instant> {
        match self {
            Self::Open { idle_since: Some(since), .. } => Some(since + BLOCK_QUIET),
            Self::Open { idle_since: None, .. } | Self::Complete { .. } => None,
        }
    }

    pub(super) fn close_if_quiet(&mut self, now: Instant) -> bool {
        if self.quiet_deadline().is_none_or(|deadline| deadline > now) {
            return false;
        }
        *self = Self::Complete { block: self.block() };
        true
    }

    pub(super) const fn is_complete(self) -> bool {
        matches!(self, Self::Complete { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    #[test]
    fn a_diff_line_for_another_block_closes_the_open_block() {
        let mut gate = BlockGate::default();
        assert!(!gate.close_before_diff(11), "nothing was open");
        assert_eq!(gate, BlockGate::Complete { block: 0 });

        gate = BlockGate::open(11);
        assert!(!gate.close_before_diff(11));
        assert_eq!(gate, BlockGate::Open { block: 11, idle_since: None });
        assert!(gate.close_before_diff(12));
        assert_eq!(gate, BlockGate::Complete { block: 11 });

        gate = BlockGate::open(12);
        assert!(gate.close_before_diff(9), "a rewritten older block also closes the open one");
        assert_eq!(gate, BlockGate::Complete { block: 12 });
    }

    #[test]
    fn a_status_line_reopens_the_last_diff_block_and_clears_idle() {
        let now = Instant::now();
        let mut gate = BlockGate::Complete { block: 11 };
        gate.on_status_line();
        assert_eq!(gate, BlockGate::Open { block: 11, idle_since: None });

        gate.on_books_idle(now);
        gate.on_status_line();
        assert_eq!(gate, BlockGate::Open { block: 11, idle_since: None });
        assert!(!gate.close_before_diff(11), "the status line did not move the block");
    }

    #[test]
    fn an_open_block_closes_once_book_streams_stay_idle_for_the_quiet_window() {
        let now = Instant::now();
        let mut gate = BlockGate::open(11);
        assert_eq!(gate.quiet_deadline(), None);
        assert!(!gate.close_if_quiet(now + 10 * MS), "streams never went idle");

        gate.on_books_idle(now);
        gate.on_books_idle(now + MS);
        assert_eq!(gate, BlockGate::Open { block: 11, idle_since: Some(now) });
        assert_eq!(gate.quiet_deadline(), Some(now + 2 * MS));
        assert!(!gate.close_if_quiet(now + MS));
        assert!(gate.close_if_quiet(now + 2 * MS));
        assert_eq!(gate, BlockGate::Complete { block: 11 });
        assert!(gate.is_complete());
        assert!(!gate.close_if_quiet(now + 3 * MS), "already closed");

        gate.on_books_idle(now + 4 * MS);
        assert_eq!(gate, BlockGate::Complete { block: 11 });
    }

    #[test]
    fn a_line_after_idle_keeps_the_block_open() {
        let now = Instant::now();
        let mut gate = BlockGate::open(11);
        gate.on_books_idle(now);
        assert!(!gate.close_before_diff(11));
        gate = BlockGate::open(11);
        assert_eq!(gate.quiet_deadline(), None);
        assert!(!gate.close_if_quiet(now + 2 * MS));
        assert!(!gate.is_complete());
    }
}
