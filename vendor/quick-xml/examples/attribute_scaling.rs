//! Small, bounded before/after CPU-exhaustion probe. Timings are evidence, not
//! a machine-dependent CI assertion. Duplicate checking stays enabled.
use quick_xml::events::BytesStart;
use std::time::Instant;

fn main() {
    for count in [8000, 16000] {
        let mut tag = String::from("e");
        for index in 0..count {
            tag.push_str(&format!(r#" a{index}="""#));
        }
        let event = BytesStart::from_content(&tag, 1);
        let start = Instant::now();
        for _ in 0..3 {
            assert_eq!(event.attributes().map(Result::unwrap).count(), count);
        }
        println!(
            "{count} attributes: {:?} mean over 3 parses",
            start.elapsed() / 3
        );
    }
}
