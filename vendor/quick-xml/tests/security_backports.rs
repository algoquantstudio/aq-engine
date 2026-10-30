//! Public API regressions for the two upstream XML denial-of-service backports.
use quick_xml::{events::Event, reader::NsReader};

fn namespaces(count: usize) -> String {
    let mut xml = String::from("<e");
    for index in 0..count {
        xml.push_str(&format!(r#" xmlns:p{index}="urn:fixture:{index}""#));
    }
    xml.push_str("/>");
    xml
}

#[test]
fn namespace_declarations_are_bounded_before_the_event_is_returned() {
    let boundary = namespaces(256);
    assert!(matches!(
        NsReader::from_str(&boundary).read_event(),
        Ok(Event::Empty(_))
    ));
    let excessive = namespaces(257);
    assert!(
        NsReader::from_str(&excessive).read_event().is_err(),
        "untrusted namespaces were all allocated and returned to the caller"
    );
}

#[test]
fn large_attribute_sets_keep_exact_duplicate_positions_and_opt_out() {
    use quick_xml::{events::attributes::AttrError, events::BytesStart};
    let mut tag = String::from("e");
    let mut first = 0;
    for index in 0..8192 {
        tag.push(' ');
        if index == 16 {
            first = tag.len();
        }
        tag.push_str(&format!(r#"a{index}="""#));
    }
    tag.push(' ');
    let repeated = tag.len();
    tag.push_str(r#"a16="duplicate""#);
    let event = BytesStart::from_content(&tag, 1);
    let mut attributes = event.attributes();
    for _ in 0..8192 {
        assert!(attributes.next().unwrap().is_ok());
    }
    assert_eq!(
        attributes.next(),
        Some(Err(AttrError::Duplicated(repeated, first)))
    );
    assert!(event
        .attributes()
        .with_checks(false)
        .all(|item| item.is_ok()));
}

#[test]
fn recovering_many_duplicate_errors_keeps_first_positions_and_iteration_state() {
    use quick_xml::{events::attributes::AttrError, events::BytesStart};
    let mut tag = String::from("e");
    let mut original = 0;
    for index in 0..4096 {
        tag.push(' ');
        original = tag.len();
        tag.push_str(&format!(r#"a{index}="""#));
    }
    let mut repetitions = Vec::new();
    for _ in 0..4096 {
        tag.push(' ');
        repetitions.push(tag.len());
        tag.push_str(r#"a4095="repeated""#);
    }
    tag.push_str(r#" final="retained""#);
    let event = BytesStart::from_content(&tag, 1);
    let mut attributes = event.attributes();
    for _ in 0..4096 {
        assert!(attributes.next().unwrap().is_ok());
    }
    let cloned = attributes.clone();
    for position in repetitions {
        assert_eq!(
            attributes.next(),
            Some(Err(AttrError::Duplicated(position, original)))
        );
    }
    let last = attributes.next().unwrap().unwrap();
    assert_eq!(last.key.as_ref(), b"final");
    assert_eq!(last.value.as_ref(), b"retained");
    assert!(attributes.next().is_none());
    assert_eq!(cloned.filter(Result::is_err).count(), 4096);
}

#[test]
fn namespace_budget_is_configurable_and_scoped_to_each_element() {
    use quick_xml::name::NamespaceError;
    let xml = namespaces(3);
    let mut reader = NsReader::from_str(&xml);
    reader.resolver_mut().set_max_declarations_per_element(2);
    assert_eq!(reader.resolver().max_declarations_per_element(), 2);
    assert!(matches!(
        reader.read_event(),
        Err(quick_xml::Error::Namespace(
            NamespaceError::TooManyDeclarations(2)
        ))
    ));
    let mut unlimited = NsReader::from_str(&xml);
    unlimited
        .resolver_mut()
        .set_max_declarations_per_element(usize::MAX);
    assert!(unlimited.read_event().is_ok());
    let child = namespaces(256);
    let siblings = format!("<root>{child}{child}</root>");
    let mut reader = NsReader::from_str(&siblings);
    loop {
        if matches!(reader.read_event().unwrap(), Event::Eof) {
            break;
        }
    }
}

#[cfg(feature = "serialize")]
#[test]
fn serde_deserialization_uses_the_same_namespace_budget() {
    #[derive(Debug, serde_derive::Deserialize)]
    struct Empty {}
    assert!(quick_xml::de::from_str::<Empty>(&namespaces(256)).is_ok());
    assert!(quick_xml::de::from_str::<Empty>(&namespaces(257)).is_err());
}

#[cfg(feature = "async-tokio")]
#[tokio::test]
async fn async_namespace_events_use_the_same_budget() {
    let xml = namespaces(257);
    let mut reader = NsReader::from_reader(xml.as_bytes());
    assert!(reader.read_event_into_async(&mut Vec::new()).await.is_err());
}
