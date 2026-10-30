//! Exact JSON wire size without allocating and discarding an encoded payload.
use serde::Serialize;
use std::io::{self, Write};

#[derive(Default)]
struct Counter(usize);

impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self.0.checked_add(bytes.len()).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "JSON encoded size overflow")
        })?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Uses the same serializer as `serde_json::to_vec`, including escaping and
/// number formatting. Suitable for admission/accounting; never estimates bytes.
pub fn json_encoded_len<T: Serialize + ?Sized>(value: &T) -> serde_json::Result<usize> {
    let mut counter = Counter::default();
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn exact_for_unicode_escaping_numbers_and_large_nested_payloads() {
        for value in [
            json!(null),
            json!(false),
            json!([]),
            json!({}),
            json!({"quote\"\\\n":"العربية 한국어 中文\t\u{0000}","nested":[1,-1,1.0000000000000002,-0.0,1e200]}),
            json!({"records":(0..100).map(|i|json!({"id":i,"message":"é\\\n".repeat(1024)})).collect::<Vec<_>>()}),
        ] {
            assert_eq!(
                json_encoded_len(&value).unwrap(),
                serde_json::to_vec(&value).unwrap().len()
            );
        }
        for value in [u64::MAX, i64::MAX as u64, 0] {
            assert_eq!(
                json_encoded_len(&value).unwrap(),
                serde_json::to_vec(&value).unwrap().len()
            );
        }
    }

    #[test]
    fn serialization_failures_and_size_overflow_still_fail_closed() {
        struct Invalid;
        impl Serialize for Invalid {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("fixture rejection"))
            }
        }
        assert!(json_encoded_len(&Invalid).is_err());
        let mut counter = Counter(usize::MAX);
        assert!(counter.write(b"x").is_err());
        assert_eq!(counter.0, usize::MAX);
    }

    #[test]
    #[ignore = "owned CPU comparison; no network or application state"]
    fn encoded_length_profile() {
        let use_counter = std::env::var("AQS_JSON_SIZE_MODE").as_deref() == Ok("counter");
        let size = std::env::var("AQS_JSON_SIZE_BYTES")
            .ok()
            .map(|s| s.parse().unwrap())
            .unwrap_or(128 * 1024);
        let iterations = std::env::var("AQS_JSON_SIZE_ITERATIONS")
            .ok()
            .map(|s| s.parse::<usize>().unwrap())
            .unwrap_or(1000);
        assert!(size <= 32 * 1024 * 1024 && (1..=100_000).contains(&iterations));
        let payload = json!({"message":"x".repeat(size),"unicode":"العربية\n", "numbers":[-0.0,1.0000000000000002,1e200]});
        let expected = json_encoded_len(&payload).unwrap();
        let started = std::time::Instant::now();
        for _ in 0..iterations {
            let length = if use_counter {
                json_encoded_len(std::hint::black_box(&payload)).unwrap()
            } else {
                serde_json::to_vec(std::hint::black_box(&payload))
                    .unwrap()
                    .len()
            };
            assert_eq!(std::hint::black_box(length), expected);
        }
        eprintln!(
            "JSON_SIZE_PROFILE {}",
            json!({"mode":if use_counter{"counter"}else{"buffer"},"iterations":iterations,"encoded_bytes":expected,"elapsed_us":started.elapsed().as_micros(),"discarded_payload_buffer_bytes_per_check":if use_counter{0}else{expected}})
        );
    }
}
