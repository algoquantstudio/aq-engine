//! RUSTSEC-2026-0253 qualifications. The before-fix probe compares pointer
//! identities only and deliberately abandons a corrupt cache before asserting,
//! so a failing regression does not traverse or drop dangling list nodes.
use super::*;
use alloc::{vec, vec::Vec};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[test]
fn pop_unlinks_the_node_before_a_key_drop_can_panic() {
    struct PanicKey {
        id: u32,
        armed: Arc<AtomicBool>,
    }
    impl Borrow<u32> for PanicKey {
        fn borrow(&self) -> &u32 {
            &self.id
        }
    }
    impl Hash for PanicKey {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.id.hash(state);
        }
    }
    impl PartialEq for PanicKey {
        fn eq(&self, other: &Self) -> bool {
            self.id == other.id
        }
    }
    impl Eq for PanicKey {}
    impl Drop for PanicKey {
        fn drop(&mut self) {
            if self.id == 3 && self.armed.swap(false, Ordering::SeqCst) {
                panic!("owned panic-on-Drop key");
            }
        }
    }
    let armed = Arc::new(AtomicBool::new(false));
    let mut cache = LruCache::new(NonZeroUsize::new(4).unwrap());
    for id in 1..=3 {
        cache.put(
            PanicKey {
                id,
                armed: armed.clone(),
            },
            id,
        );
    }
    // Both reads are of live allocations before pop. After the panic we only
    // inspect the always-live head sentinel, never a possibly-freed entry.
    let expected_next = unsafe { (*(*cache.head).next).next };
    armed.store(true, Ordering::SeqCst);
    let panicked = catch_unwind(AssertUnwindSafe(|| cache.pop(&3))).is_err();
    let actual_next = unsafe { (*cache.head).next };
    if actual_next != expected_next {
        std::mem::forget(cache);
        panic!("popped node remains linked after its key destructor panics");
    }
    assert!(panicked);
    assert_eq!(cache.len(), 2);
    assert_eq!(
        cache
            .iter()
            .map(|(key, value)| (key.id, *value))
            .collect::<Vec<_>>(),
        vec![(2, 2), (1, 1)]
    );
}

// Upstream PR238 regression pattern, plus an explicit check that Drop panicked.
#[test]
fn upstream_pop_panicking_key_drop_keeps_list_consistent() {
    static ARMED: AtomicBool = AtomicBool::new(false);
    #[derive(PartialEq, Eq, Hash)]
    struct PanicKey(u32);
    impl Drop for PanicKey {
        fn drop(&mut self) {
            if ARMED.swap(false, Ordering::SeqCst) {
                panic!("PanicKey::drop");
            }
        }
    }
    let mut cache = LruCache::new(NonZeroUsize::new(4).unwrap());
    cache.put(PanicKey(1), "a");
    cache.put(PanicKey(2), "b");
    cache.put(PanicKey(3), "c");
    ARMED.store(true, Ordering::SeqCst);
    assert!(catch_unwind(AssertUnwindSafe(|| cache.pop(&PanicKey(2)))).is_err());
    cache.put(PanicKey(4), "d");
    cache.put(PanicKey(5), "e");
    assert_eq!(cache.get(&PanicKey(3)), Some(&"c"));
    assert_eq!(cache.iter().count(), cache.len());
}
