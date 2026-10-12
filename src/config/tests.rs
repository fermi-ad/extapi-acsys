//! Tests for the config module
//!
//! This is the only module that relies on the external environment.
//! To ensure that each test case has a good starting place, the [`serial_test`]
//! crate is used. It forces the test cases to run sequentially, eliminating
//! the possibility of race conditions when different test cases need the environment
//! to be configured in different ways.

use serial_test::serial;

use super::*;

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
        alarms_kafka,
        alarms_svc,
        blm,
        clock,
        devdb,
        tlg,
        unr,
        wscan,
    } = &*get_global_config();

    assert_eq!(enabled(alarms_db).host_addr, "test host");
    assert_eq!(enabled(alarms_kafka).host_addr, "test host");
    assert_eq!(enabled(alarms_kafka).topic, "test topic");
    assert_eq!(enabled(alarms_svc).host_addr, "test host");
    assert_eq!(enabled(blm).host_addr, "test host");
    assert_eq!(enabled(clock).host_addr, "test host");
    assert_eq!(enabled(devdb).host_addr, "test host");
    assert_eq!(enabled(tlg).host_addr, "test host");
    assert_eq!(enabled(unr).host_addr, "test host");
    assert_eq!(enabled(wscan).host_addr, "test host");
}

#[test]
#[serial]
fn upstream_disabled_when_var_missing() {
    let config = with_env(&["DEVDB_GRPC_HOST"], None, get_global_config);

    let info = config.devdb.disabled_info().expect("devdb is disabled");
    assert_eq!(info.feature, "DevDB");
    assert_eq!(info.missing_vars, ["DEVDB_GRPC_HOST"]);
    // Other upstreams are unaffected.
    assert!(config.tlg.require().is_ok());
}

#[test]
#[serial]
fn upstream_disabled_when_var_blank() {
    let config = with_env(&["BLM_GRPC_HOST"], Some("  "), get_global_config);

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
        "SCANNER_GRPC_HOST",
        "TLG_GRPC_HOST",
        "UNR_GRPC_HOST",
    ];
    let config = with_env(&vars, None, get_global_config);

    assert!(config.alarms_db.require().is_err());
    assert!(config.alarms_svc.require().is_err());
    assert!(config.blm.require().is_err());
    assert!(config.clock.require().is_err());
    assert!(config.devdb.require().is_err());
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
        get_global_config,
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
    let config = with_env(&["ALARMS_KAFKA_TOPIC"], None, get_global_config);

    let info = config.alarms_kafka.disabled_info().expect("kafka disabled");
    assert_eq!(info.missing_vars, ["ALARMS_KAFKA_TOPIC"]);
}

#[test]
fn disabled_upstream_maps_to_graphql_and_grpc_errors() {
    let up: Upstream<GrpcConfig> =
        Upstream::disabled("BLM", &["BLM_GRPC_HOST"]);

    let gql = up.require_gql().unwrap_err();
    assert_eq!(
        gql.message,
        "The BLM feature is not available in this deployment"
    );
    assert!(gql.extensions.is_some());

    let status = tonic::Status::from(up.require().unwrap_err());
    assert_eq!(status.code(), tonic::Code::Unimplemented);
}
