//! Context isolation and fixed-binding regressions for the codec used by Polars.
use std::io::{Read, Write};

#[cfg(feature = "legacy")]
#[test]
fn failed_legacy_frame_does_not_commit_uninitialized_output_capacity() {
    // Upstream zstd-safe's reserved-bit legacy-header regression. Never inspect
    // uninitialized bytes: only the buffer positions and public vector length.
    let corrupt = [0x27u8, 0xb5, 0x2f, 0xfd, 0x08, 0x00];
    let mut context = zstd::zstd_safe::DCtx::create();
    let mut buffer = Vec::with_capacity(1024);
    let mut input = zstd::zstd_safe::InBuffer::around(&corrupt);
    let mut output = zstd::zstd_safe::OutBuffer::around(&mut buffer);
    assert!(context.decompress_stream(&mut output, &mut input).is_err());
    assert_eq!(
        output.pos(),
        0,
        "a failed frame must not expose output capacity"
    );
    assert_eq!(
        input.pos(),
        0,
        "a failed frame must not commit input position"
    );
    drop(output);
    assert!(buffer.is_empty());
}

#[cfg(feature = "legacy")]
#[test]
fn failed_legacy_writer_never_forwards_output_on_flush_or_drop() {
    let corrupt = [0x27u8, 0xb5, 0x2f, 0xfd, 0x08, 0x00];
    let mut output = Vec::new();
    {
        let mut decoder =
            zstd::stream::write::Decoder::new(&mut output).unwrap();
        assert!(decoder.write_all(&corrupt).is_err());
        let _ = decoder.flush();
    }
    assert!(
        output.is_empty(),
        "failed legacy input must not forward stale heap capacity"
    );
}

#[test]
fn prepared_dictionaries_still_round_trip_while_their_owners_are_alive() {
    let bytes =
        b"a shared compression dictionary with repeated research fields";
    let encoder_dictionary = zstd::dict::EncoderDictionary::copy(bytes, 3);
    let decoder_dictionary = zstd::dict::DecoderDictionary::copy(bytes);
    let payload = b"research fields: symbol, price, quantity, return, account, timestamp";
    let mut encoder = zstd::stream::write::Encoder::with_prepared_dictionary(
        Vec::new(),
        &encoder_dictionary,
    )
    .unwrap();
    encoder.write_all(payload).unwrap();
    let encoded = encoder.finish().unwrap();
    let mut decoder = zstd::stream::read::Decoder::with_prepared_dictionary(
        encoded.as_slice(),
        &decoder_dictionary,
    )
    .unwrap();
    let mut decoded = Vec::new();
    decoder.read_to_end(&mut decoded).unwrap();
    assert_eq!(decoded, payload);
}

#[test]
fn abandoned_encoder_does_not_prefix_the_next_jobs_plaintext() {
    let mut context = zstd::zstd_safe::CCtx::create();
    zstd::Encoder::with_context(Vec::new(), &mut context)
        .write_all(b"private first job")
        .unwrap();
    let mut next = zstd::Encoder::with_context(Vec::new(), &mut context);
    next.write_all(b"independent second job").unwrap();
    let encoded = next.finish().unwrap();
    assert_eq!(
        zstd::decode_all(&encoded[..]).unwrap(),
        b"independent second job"
    );
}

#[test]
fn abandoned_decoder_does_not_return_the_previous_jobs_remaining_plaintext() {
    let first = zstd::encode_all(&b"private first job"[..], 1).unwrap();
    let second = zstd::encode_all(&b"independent second job"[..], 1).unwrap();
    let mut context = zstd::zstd_safe::DCtx::create();
    zstd::Decoder::with_context(&first[..], &mut context)
        .read_exact(&mut [0; 5])
        .unwrap();
    let mut decoded = Vec::new();
    zstd::Decoder::with_context(&second[..], &mut context)
        .read_to_end(&mut decoded)
        .unwrap();
    assert_eq!(decoded, b"independent second job");
}
