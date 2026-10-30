use super::*;

#[cfg(unix)]
#[test]
fn private_permission_protection_never_follows_a_link() {
    use std::os::unix::fs::{symlink,PermissionsExt};
    let root=std::env::temp_dir().join(format!("aqe-private-permission-{}",uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let outside=root.join("unrelated");
    std::fs::write(&outside,b"unchanged").unwrap();
    std::fs::set_permissions(&outside,std::fs::Permissions::from_mode(0o644)).unwrap();
    let link=root.join("linked");
    symlink(&outside,&link).unwrap();
    assert!(protect_private_path(&link,false).is_err());
    assert_eq!(std::fs::metadata(&outside).unwrap().permissions().mode()&0o777,0o644);
    std::fs::remove_dir_all(root).unwrap();
}

fn fixture() -> LaunchConfig {
    LaunchConfig {
        version: VERSION,
        run_id: uuid::Uuid::new_v4().to_string(),
        cloud: Some(CloudConfig {
            url: "ws://127.0.0.1:9081".into(),
            session_id: uuid::Uuid::new_v4().to_string(),
            session_secret: "fixture-secret-never-log".into(),
            strategy_id: "fixture-strategy".into(),
            user_id: "users:fixture".into(),
            node_id: "local".into(),
            access_method: "aqe_live".into(),
        }),
        mt5: Some(Mt5Config {
            bind_addr: "127.0.0.1:18080".into(),
            token: "fixture-mt5-token".into(),
        }),
        delivery: Some(DeliveryConfig::EmbeddedGateway {
            url: "http://127.0.0.1:9080".into(),
            outbox_directory: "/fixture/outboxes".into(),
            account_coordinator_dir: "/fixture/account-locks".into(),
        }),
        fill_export: true,
        capture:None,
    }
}

#[test]
fn schema_keeps_one_session_and_default_access_and_no_legacy_flags() {
    let config = fixture();
    let json = serde_json::to_string(&config).unwrap();
    assert!(!json.contains("access_method"));
    assert!(!json.contains("live_session_id"));
    assert!(!json.contains("AQE_EXECUTION_EVIDENCE_EXPORT"));
    assert!(!json.contains("AQE_POSITION_MARK"));
    assert!(!json.contains("\"capture\""),"old/default launch files remain unchanged");
    let restored: LaunchConfig = serde_json::from_str(&json).unwrap();
    restored.validate().unwrap();
    assert_eq!(restored.cloud.unwrap().access_method, "aqe_live");
}

fn capture_fixture()->CloudCaptureConfig{
    let physical=CapturePartitionConfig{physical_limit_bytes:64*1024*1024,operational_headroom_bytes:MIN_CAPTURE_OPERATIONAL_HEADROOM_BYTES};
    let outbox=CaptureOutboxConfig{payload_limit_bytes:4*1024*1024,physical};
    CloudCaptureConfig{version:1,source_journal:physical,publisher:outbox,embedded_receiver:Some(outbox)}
}

#[test]
fn protected_capture_has_explicit_transport_appropriate_partitions(){
    let mut config=fixture();
    let policy=capture_fixture();
    config.capture=Some(policy);
    config.validate().unwrap();
    let restored:LaunchConfig=serde_json::from_str(&serde_json::to_string(&config).unwrap()).unwrap();
    assert_eq!(restored.capture,Some(policy));
    assert_eq!(restored.setting("AQE_CAPTURE_POLICY"),None,"no new launch environment flags");
    config.capture.as_mut().unwrap().embedded_receiver=None;
    assert!(config.validate().is_err(),"embedded delivery must budget its owned receiver");
    config.delivery=Some(DeliveryConfig::ManagedNats{
        runtime_id:uuid::Uuid::new_v4().to_string(),url:"nats://127.0.0.1:4222".into(),token:"fixture".into(),
        outbox_path:"/fixture/publisher.db".into(),account_coordinator_dir:"/fixture/locks".into(),split_insight_history:true,
    });
    config.validate().unwrap();
    config.capture.as_mut().unwrap().embedded_receiver=policy.embedded_receiver;
    assert!(config.validate().is_err(),"a strategy cannot assign a private budget to the shared host receiver");
}

#[test]
fn protected_capture_rejects_unknown_unsafe_or_unowned_policies(){
    let mut config=fixture();config.capture=Some(capture_fixture());
    config.capture.as_mut().unwrap().version=2;
    assert!(config.validate().is_err());
    config.capture=Some(capture_fixture());
    config.capture.as_mut().unwrap().source_journal.operational_headroom_bytes=1;
    assert!(config.validate().is_err());
    config.capture=Some(capture_fixture());
    config.capture.as_mut().unwrap().publisher.payload_limit_bytes=0;
    assert!(config.validate().is_err());
    config.capture=Some(capture_fixture());
    config.cloud.as_mut().unwrap().strategy_id="x".repeat(129);
    assert!(config.validate().is_err());
    config.cloud=None;config.delivery=None;
    assert!(config.validate().is_err(),"standalone trading cannot inherit cloud capture policy");
    let mut value=serde_json::to_value(capture_fixture()).unwrap();value["unknown"]=true.into();
    assert!(serde_json::from_value::<CloudCaptureConfig>(value).is_err());
}

#[cfg(not(target_arch="wasm32"))]
#[test]
fn protected_capture_private_file_roundtrip_preserves_exact_allocation(){
    let root=std::env::temp_dir().join(format!("capture-launch-{}",uuid::Uuid::new_v4()));
    let path=root.join("launch.json");let mut config=fixture();config.capture=Some(capture_fixture());
    write_private(&path,&config).unwrap();
    let restored=read_private(&path).unwrap();assert_eq!(restored.capture,config.capture);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn aqs_policy_disables_state_archive_without_a_generated_environment_flag() {
    let config = fixture();
    assert_eq!(
        config.setting("AQE_EXECUTION_EVIDENCE_EXPORT").as_deref(),
        Some("false")
    );
    assert_eq!(
        config.setting("AQE_EXECUTION_FILL_EXPORT").as_deref(),
        Some("true")
    );
    assert_eq!(
        config.setting("AQE_RUN_ID").as_deref(),
        Some(config.run_id.as_str())
    );
    assert_eq!(
        config.setting("AQE_MT5_BRIDGE_TOKEN").as_deref(),
        Some("fixture-mt5-token")
    );
    assert_eq!(config.setting("UNRELATED_SETTING"), None);
}

#[test]
fn nats_and_embedded_delivery_are_explicit_and_mutually_exclusive() {
    let mut config = fixture();
    config.delivery = Some(DeliveryConfig::ManagedNats {
        runtime_id: uuid::Uuid::new_v4().to_string(),
        url: "nats://127.0.0.1:4222".into(),
        token: "private".into(),
        outbox_path: "C:\\AQS Runtime\\publisher.db".into(),
        account_coordinator_dir: "C:\\AQS Runtime\\locks".into(),
        split_insight_history: true,
    });
    config.validate().unwrap();
    assert_eq!(config.setting("AQE_EVENT_BUS").as_deref(), Some("nats"));
    assert_eq!(
        config.setting("AQE_SPLIT_INSIGHT_HISTORY").as_deref(),
        Some("true")
    );
    config.cloud = None;
    assert!(config.validate().is_err());
}

#[test]
fn rejects_unknown_versions_missing_delivery_and_unknown_fields_without_echoing_secrets() {
    let mut config = fixture();
    config.version += 1;
    assert!(config.validate().is_err());
    config.version = VERSION;
    config.delivery = None;
    assert!(config.validate().is_err());
    let mut value = serde_json::to_value(fixture()).unwrap();
    value["unrecognized"] = "private".into();
    assert!(serde_json::from_value::<LaunchConfig>(value).is_err());
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn private_roundtrip_is_bounded_immutable_and_cli_only_remains_supported() {
    let directory = std::env::temp_dir().join(format!("aqe-launch-test-{}", uuid::Uuid::new_v4()));
    let path = directory.join("launch config.json");
    let config = fixture();
    write_private(&path, &config).unwrap();
    let restored = read_private(&path).unwrap();
    assert_eq!(restored.run_id, config.run_id);
    assert!(
        write_private(&path, &config).is_err(),
        "never overwrite a live launch file"
    );
    assert!(
        process::from_args(["strategy".into(), "--live".into()])
            .unwrap()
            .is_none()
    );
    let args = vec![
        "strategy".into(),
        "--live".into(),
        ARGUMENT.into(),
        path.to_string_lossy().into_owned(),
    ];
    assert!(process::from_args(args.clone()).unwrap().is_some());
    let mut conflicting = args.clone();
    conflicting.extend(["--session-secret".into(), "private".into()]);
    let error = process::from_args(conflicting).err().unwrap();
    assert!(!error.contains("private"));
    let mut repeated = args;
    repeated.extend([ARGUMENT.into(), path.to_string_lossy().into_owned()]);
    assert!(process::from_args(repeated).is_err());
    assert!(process::from_args(["strategy".into(), ARGUMENT.into()]).is_err());
    std::fs::remove_dir_all(directory).unwrap();
}

#[cfg(unix)]
#[test]
fn rejects_public_permissions_symlinks_and_oversized_files() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let directory =
        std::env::temp_dir().join(format!("aqe-launch-private-test-{}", uuid::Uuid::new_v4()));
    let path = directory.join("launch.json");
    write_private(&path, &fixture()).unwrap();
    assert_eq!(
        directory.metadata().unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(path.metadata().unwrap().permissions().mode() & 0o777, 0o600);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(read_private(&path).is_err());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let link = directory.join("link.json");
    symlink(&path, &link).unwrap();
    assert!(read_private(&link).is_err());
    std::fs::write(&path, vec![b' '; MAX_BYTES + 1]).unwrap();
    assert!(read_private(&path).is_err());
    std::fs::remove_dir_all(directory).unwrap();
}
