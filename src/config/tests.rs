//! Tests for the config module
//!
//! This is the only module that relies on the external environment.
//! To ensure that each test case has a good starting place, the [`serial_test`]
//! crate is used. It forces the test cases to run sequentially, eliminating
//! the possibility of race conditions when different test cases need the environment
//! to be configured in different ways.

use serial_test::serial;

use super::*;

fn global_config() -> Arc<ExtapiGlobalConfig> {
    get_global_config().expect("REQUIRED_UPSTREAMS is unset in tests")
}

fn enabled<T>(upstream: &Upstream<T>) -> &T {
    upstream.require().expect("upstream should be enabled")
}

/// Runs `f` with `vars` set to `value` (or unset when `None`), then restores
/// the original test values from `.cargo/config.toml`.
fn with_env<R>(vars: &[&str], value: Option<&str>, f: impl FnOnce() -> R) -> R {
    let saved: Vec<_> = vars.iter().map(|v| std::env::var(v).ok()).collect();
    for var in vars {
        // SAFETY: tests touching the environment are serialized with `#[serial]`.
        unsafe {
            match value {
                Some(v) => std::env::set_var(var, v),
                None => std::env::remove_var(var),
            }
        }
    }
    let result = f();
    for (var, old) in vars.iter().zip(saved) {
        // SAFETY: see above.
        unsafe {
            match old {
                Some(v) => std::env::set_var(var, v),
                None => std::env::remove_var(var),
            }
        }
    }
    result
}

#[test]
#[serial]
fn global_config_populates_when_all_vals_present() {
    let ExtapiGlobalConfig {
        alarms_db,
        #[cfg(feature = "kafka")]
        alarms_kafka,
        alarms_svc,
        blm,
        clock,
        devdb,
        mi8_autotune,
        tlg,
        unr,
        wscan,
    } = &*get_global_config().expect("config is valid");

    assert_eq!(enabled(alarms_db).host_addr, "test host");
    #[cfg(feature = "kafka")]
    {
        assert_eq!(enabled(alarms_kafka).host_addr, "test host");
        assert_eq!(enabled(alarms_kafka).topic, "test topic");
    }
    assert_eq!(enabled(alarms_svc).host_addr, "test host");
    assert_eq!(enabled(blm).host_addr, "test host");
    assert_eq!(enabled(clock).host_addr, "test host");
    assert_eq!(enabled(devdb).host_addr, "test host");
    assert_eq!(enabled(mi8_autotune).host_addr, "test host");
    assert_eq!(enabled(tlg).host_addr, "test host");
    assert_eq!(enabled(unr).host_addr, "test host");
    assert_eq!(enabled(wscan).host_addr, "test host");
}

#[test]
#[serial]
fn upstream_disabled_when_var_missing() {
    let config = with_env(&["DEVDB_GRPC_HOST"], None, global_config);

    let info = config.devdb.disabled_info().expect("devdb is disabled");
    assert_eq!(info.feature, "DevDB");
    assert_eq!(info.missing_vars, ["DEVDB_GRPC_HOST"]);
    // Other upstreams are unaffected.
    assert!(config.tlg.require().is_ok());
}

#[test]
#[serial]
fn upstream_disabled_when_var_blank() {
    let config = with_env(&["BLM_GRPC_HOST"], Some("  "), global_config);

    let info = config.blm.disabled_info().expect("blm is disabled");
    assert_eq!(info.feature, "BLM");
    assert_eq!(info.missing_vars, ["BLM_GRPC_HOST"]);
}

#[test]
#[serial]
fn startup_succeeds_with_no_upstreams_configured() {
    let vars = [
        "ALARMS_KAFKA_HOST",
        "ALARMS_KAFKA_TOPIC",
        "BLM_GRPC_HOST",
        "CLOCK_GRPC_HOST",
        "DEVDB_GRPC_HOST",
        "GRPC_ALARMS_DB_HOST",
        "GRPC_ALARMS_SERVICE_HOST",
        "MI8_AUTOTUNE_GRPC_HOST",
        "SCANNER_GRPC_HOST",
        "TLG_GRPC_HOST",
        "UNR_GRPC_HOST",
    ];
    let config = with_env(&vars, None, global_config);

    assert!(config.alarms_db.require().is_err());
    assert!(config.alarms_svc.require().is_err());
    assert!(config.blm.require().is_err());
    assert!(config.clock.require().is_err());
    assert!(config.devdb.require().is_err());
    assert!(config.mi8_autotune.require().is_err());
    assert!(config.tlg.require().is_err());
    assert!(config.unr.require().is_err());
    assert!(config.wscan.require().is_err());
}

#[cfg(feature = "kafka")]
#[test]
#[serial]
fn kafka_disabled_reports_every_missing_var() {
    let config = with_env(
        &["ALARMS_KAFKA_HOST", "ALARMS_KAFKA_TOPIC"],
        None,
        global_config,
    );

    let info = config.alarms_kafka.disabled_info().expect("kafka disabled");
    assert_eq!(
        info.missing_vars,
        ["ALARMS_KAFKA_HOST", "ALARMS_KAFKA_TOPIC"]
    );
}

#[cfg(feature = "kafka")]
#[test]
#[serial]
fn kafka_disabled_when_only_topic_missing() {
    let config = with_env(&["ALARMS_KAFKA_TOPIC"], None, global_config);

    let info = config.alarms_kafka.disabled_info().expect("kafka disabled");
    assert_eq!(info.missing_vars, ["ALARMS_KAFKA_TOPIC"]);
}

#[test]
fn feature_disabled_message_names_feature_not_var() {
    let up: Upstream<GrpcConfig> = Upstream::disabled("BLM", ["BLM_GRPC_HOST"]);

    let message = up.require().unwrap_err().message();
    assert_eq!(
        message,
        "The BLM feature is not available in this deployment"
    );
    assert!(!message.contains("BLM_GRPC_HOST"));
}

// REQUIRED_UPSTREAMS

fn with_disabled_devdb_and_unr() -> ExtapiGlobalConfig {
    let mut config = get_test_config();
    config.devdb = Upstream::disabled("DevDB", ["DEVDB_GRPC_HOST"]);
    config.unr = Upstream::disabled("UNR", ["UNR_GRPC_HOST"]);
    config
}

#[test]
fn required_upstreams_unset_or_blank_allows_everything_disabled() {
    let config = with_disabled_devdb_and_unr();
    assert_eq!(config.check_required(None), Ok(()));
    assert_eq!(config.check_required(Some(" , ")), Ok(()));
}

#[test]
fn required_upstreams_pass_when_listed_upstreams_are_enabled() {
    let config = with_disabled_devdb_and_unr();
    assert_eq!(config.check_required(Some("blm, clock")), Ok(()));
}

#[test]
fn required_upstreams_report_every_missing_upstream() {
    let config = with_disabled_devdb_and_unr();

    let err = config.check_required(Some("unr,blm,devdb")).unwrap_err();
    assert_eq!(
        err,
        ConfigError::MissingRequiredUpstreams(vec![
            Disabled {
                feature: "DevDB",
                missing_vars: vec!["DEVDB_GRPC_HOST"],
            },
            Disabled {
                feature: "UNR",
                missing_vars: vec!["UNR_GRPC_HOST"],
            },
        ])
    );
    assert_eq!(
        err.to_string(),
        "required upstream(s) not configured: DevDB (set DEVDB_GRPC_HOST); \
         UNR (set UNR_GRPC_HOST)"
    );
}

#[test]
fn required_upstreams_reject_unknown_keys() {
    let config = get_test_config();

    let err = config
        .check_required(Some("devdb, dev_db, UNR"))
        .unwrap_err();
    let ConfigError::UnknownRequiredUpstreams { unknown, known } = &err else {
        panic!("expected unknown-key error, got {err:?}");
    };
    assert_eq!(unknown, &["dev_db", "UNR"]);
    assert!(known.contains(&"devdb"));
    assert!(err.to_string().starts_with(
        "REQUIRED_UPSTREAMS names unknown upstream(s): dev_db, UNR;"
    ));
}

#[test]
#[serial]
fn get_global_config_fails_when_required_upstream_missing() {
    let result = with_env(&["DEVDB_GRPC_HOST"], None, || {
        with_env(&["REQUIRED_UPSTREAMS"], Some("devdb"), get_global_config)
    });

    assert!(matches!(
        result,
        Err(ConfigError::MissingRequiredUpstreams(ref missing))
            if missing.len() == 1 && missing[0].feature == "DevDB"
    ));
}

#[cfg(not(feature = "kafka"))]
#[test]
fn alarms_kafka_is_not_a_known_upstream_without_kafka_feature() {
    let err = get_test_config()
        .check_required(Some("alarms_kafka"))
        .unwrap_err();
    assert!(matches!(err, ConfigError::UnknownRequiredUpstreams { .. }));
}
