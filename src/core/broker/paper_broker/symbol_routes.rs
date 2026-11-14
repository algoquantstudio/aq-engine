//! Private OMS membership. Admission ordinals preserve FIFO across symbol pages;
//! a sparse bar never visits orders belonging only to other symbols.
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

#[derive(Default)]
pub(super) struct SymbolOrders {
    by_symbol: HashMap<String, BTreeMap<u64, Arc<str>>>,
    owners: HashMap<Arc<str>, (String, u64)>,
    #[cfg(test)]
    next: u64,
}

impl SymbolOrders {
    #[cfg(test)]
    pub fn insert(&mut self, symbol: &str, id: String) {
        let ordinal = self.next;
        self.next = self
            .next
            .checked_add(1)
            .expect("paper admission order exhausted");
        self.insert_at(symbol, id.into(), ordinal);
    }

    pub fn insert_at(&mut self, symbol: &str, id: Arc<str>, ordinal: u64) {
        self.remove(&id);
        self.by_symbol
            .entry(symbol.to_owned())
            .or_default()
            .insert(ordinal, id.clone());
        self.owners.insert(id, (symbol.to_owned(), ordinal));
    }

    pub fn remove(&mut self, id: &str) {
        let Some((symbol, ordinal)) = self.owners.remove(id) else {
            return;
        };
        let rows = self
            .by_symbol
            .get_mut(&symbol)
            .expect("indexed order symbol");
        rows.remove(&ordinal);
        if rows.is_empty() {
            self.by_symbol.remove(&symbol);
        }
    }

    pub fn select<'a>(&self, symbols: impl Iterator<Item = &'a String>) -> Vec<(u64, Arc<str>)> {
        let mut selected = Vec::new();
        let mut groups = 0;
        for symbol in symbols {
            if let Some(rows) = self.by_symbol.get(symbol) {
                groups += 1;
                selected.extend(rows.iter().map(|(ordinal, id)| (*ordinal, id.clone())));
            }
        }
        if groups > 1 {
            selected.sort_unstable_by_key(|(ordinal, _)| *ordinal);
        }
        selected
    }

    pub fn is_empty(&self) -> bool {
        self.owners.is_empty()
    }
}

/// A dense single-symbol stream needs only a FIFO. Promote to symbol indexes
/// when a second pending symbol appears, retaining the original admission
/// ordinals. Active bracket routes use their separate canonical-order index.
#[derive(Default)]
pub(super) struct PendingOrders {
    symbol: Option<String>,
    fifo: VecDeque<(u64, String)>,
    indexed: Option<SymbolOrders>,
    next: u64,
}

impl PendingOrders {
    pub fn insert(&mut self, symbol: &str, id: String) {
        let ordinal = self.next;
        self.next = self
            .next
            .checked_add(1)
            .expect("paper admission order exhausted");
        if let Some(indexed) = &mut self.indexed {
            indexed.insert_at(symbol, id.into(), ordinal);
            return;
        }
        if !self.fifo.is_empty() && self.symbol.as_deref() != Some(symbol) {
            let mut indexed = SymbolOrders::default();
            let prior_symbol = self.symbol.take().expect("pending FIFO symbol");
            for (ordinal, id) in std::mem::take(&mut self.fifo) {
                indexed.insert_at(&prior_symbol, id.into(), ordinal);
            }
            indexed.insert_at(symbol, id.into(), ordinal);
            self.indexed = Some(indexed);
        } else {
            if self.symbol.as_deref() != Some(symbol) {
                self.symbol = Some(symbol.to_owned());
            }
            self.fifo.push_back((ordinal, id));
        }
    }

    /// Visit eligible IDs in FIFO order and retain only unfinished orders.
    /// The dense path borrows IDs in-place: no per-step ID clones or per-fill
    /// tree/hash removals. Sparse indexed batches keep their existing bounds.
    pub fn retain_for_symbols<'a>(
        &mut self,
        mut symbols: impl Iterator<Item = &'a String>,
        mut retain: impl FnMut(&str) -> bool,
    ) -> usize {
        if let Some(indexed) = &mut self.indexed {
            let selected = indexed.select(symbols);
            let visited = selected.len();
            for (_, id) in selected {
                if !retain(&id) {
                    indexed.remove(&id);
                }
            }
            if indexed.is_empty() {
                self.indexed = None;
            }
            return visited;
        }
        let Some(symbol) = &self.symbol else {
            return 0;
        };
        if self.fifo.is_empty() || !symbols.any(|available| available == symbol) {
            return 0;
        }
        let visited = self.fifo.len();
        self.fifo.retain(|(_, id)| retain(id));
        visited
    }

    pub fn remove_many(&mut self, ids: &HashSet<String>) {
        if let Some(indexed) = &mut self.indexed {
            for id in ids {
                indexed.remove(id);
            }
            if indexed.is_empty() {
                self.indexed = None;
            }
        } else {
            // One stable pass for a cancellation/release batch, not one scan
            // per ID. Remaining IDs retain their original admission ordinals.
            self.fifo.retain(|(_, id)| !ids.contains(id));
        }
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.fifo.is_empty() && self.indexed.as_ref().is_none_or(SymbolOrders::is_empty)
    }

    #[cfg(test)]
    pub fn is_indexed(&self) -> bool {
        self.indexed.is_some()
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct PositionTotals {
    pub pnl: f64,
    pub margin: f64,
}

#[derive(Default)]
pub(super) struct ActiveRoutes {
    pub orders: SymbolOrders,
    pub dirty: bool,
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct WorkCounts {
    pub pending: std::sync::atomic::AtomicUsize,
    pub active: std::sync::atomic::AtomicUsize,
    pub positions: std::sync::atomic::AtomicUsize,
    pub active_reindex: std::sync::atomic::AtomicUsize,
}
