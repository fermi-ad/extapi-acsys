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
    } = &*get_global_config()
        .expect("All test env vars are set in .cargo/config.toml");

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
fn global_config_returns_err_when_val_missing() {
    let result;
    unsafe {
        std::env::remove_var("DEVDB_GRPC_HOST");
        result = get_global_config();
        std::env::set_var("DEVDB_GRPC_HOST", "test host");
    }

    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Environment variable 'DEVDB_GRPC_HOST' is not set"
    );
}

#[test]
#[serial]
fn global_config_returns_err_when_val_blank() {
    let result;
    unsafe {
        std::env::set_var("DEVDB_GRPC_HOST", "   ");
        result = get_global_config();
        std::env::set_var("DEVDB_GRPC_HOST", "test host");
    }

    assert_eq!(
        result.unwrap_err().to_string(),
        "Environment variable 'DEVDB_GRPC_HOST' is not set"
    );
}

#[test]
#[serial]
fn optional_upstream_disabled_when_var_missing_or_blank() {
    let (missing, blank);
    unsafe {
        std::env::remove_var("BLM_GRPC_HOST");
        missing = get_global_config();
        std::env::set_var("BLM_GRPC_HOST", "  ");
        blank = get_global_config();
        std::env::set_var("BLM_GRPC_HOST", "test host");
    }

    for result in [missing, blank] {
        let config = result.expect("optional upstream must not fail startup");
        let info = config.blm.disabled_info().expect("blm is disabled");
        assert_eq!(info.feature, "BLM");
        assert_eq!(info.missing_vars, ["BLM_GRPC_HOST"]);
    }
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
