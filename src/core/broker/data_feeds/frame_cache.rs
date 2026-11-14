//! Bounded immutable history frames. Clones share Polars column buffers.
use polars::prelude::DataFrame;
use std::collections::VecDeque;

pub(super) struct FrameCache {
    entries: VecDeque<(String, DataFrame, usize)>,
    bytes: usize,
    max_bytes: usize,
    max_entries: usize,
}

impl FrameCache {
    pub(super) fn new(max_bytes: usize, max_entries: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            bytes: 0,
            max_bytes,
            max_entries,
        }
    }

    pub(super) fn get(&mut self, key: &str) -> Option<DataFrame> {
        let position = self.entries.iter().position(|entry| entry.0 == key)?;
        let entry = self.entries.remove(position)?;
        let frame = entry.1.clone();
        self.entries.push_back(entry);
        Some(frame)
    }

    pub(super) fn insert(&mut self, key: String, frame: DataFrame) {
        if let Some(position) = self.entries.iter().position(|entry| entry.0 == key) {
            self.bytes -= self.entries.remove(position).unwrap().2;
        }
        // Include per-column/key overhead as well as the visible Arrow buffers.
        let bytes = frame
            .estimated_size()
            .saturating_add(key.capacity())
            .saturating_add(frame.width().saturating_mul(256));
        if bytes > self.max_bytes || self.max_entries == 0 {
            return;
        }
        while self.entries.len() >= self.max_entries
            || self.bytes.saturating_add(bytes) > self.max_bytes
        {
            let Some((_, _, removed)) = self.entries.pop_front() else {
                break;
            };
            self.bytes -= removed;
        }
        self.bytes += bytes;
        self.entries.push_back((key, frame, bytes));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    #[test]
    fn frame_cache_shares_columns_and_isolates_consumer_mutations() {
        let original = df!("close" => [1.0, 2.0]).unwrap();
        let pointer = original
            .column("close")
            .unwrap()
            .f64()
            .unwrap()
            .cont_slice()
            .unwrap()
            .as_ptr();
        let mut cache = FrameCache::new(4096, 2);
        cache.insert("a".into(), original);
        let mut first = cache.get("a").unwrap();
        assert_eq!(
            pointer,
            first
                .column("close")
                .unwrap()
                .f64()
                .unwrap()
                .cont_slice()
                .unwrap()
                .as_ptr()
        );
        first
            .with_column(Column::new("close".into(), [99.0, 99.0]))
            .unwrap();
        assert_eq!(
            cache
                .get("a")
                .unwrap()
                .column("close")
                .unwrap()
                .f64()
                .unwrap()
                .get(0),
            Some(1.0)
        );
    }

    #[test]
    fn frame_cache_evicts_lru_and_bounds_retained_bytes() {
        let small = df!("close" => [1.0]).unwrap();
        let mut cache = FrameCache::new(1024, 2);
        cache.insert("a".into(), small.clone());
        cache.insert("b".into(), small.clone());
        assert!(cache.get("a").is_some());
        cache.insert("c".into(), small.clone());
        assert!(cache.get("b").is_none());
        cache.insert("a".into(), small);
        assert_eq!(cache.entries.len(), 2);
        cache.insert("large".into(), df!("close" => vec![0.0; 1024]).unwrap());
        assert!(cache.get("large").is_none());
        assert!(cache.bytes <= cache.max_bytes);
    }
}
