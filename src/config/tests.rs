//! Tests for the config module
//!
//! This is the only module that relies on the external environment.
//! To ensure that each test case has a good starting place, the [`serial_test`]
//! crate is used. It forces the test cases to run sequentially, eliminating
//! the possibility of race conditions when different test cases need the environment
//! to be configured in different ways.

use serial_test::serial;

use super::*;

#[test]
#[serial]
fn global_config_populates_when_all_vals_present() {
    let ExtapiGlobalConfig {
        alarms_db,
        alarms_kafka,
        alarms_svc,
        clock,
        devdb,
        tlg,
        unr,
        wscan,
    } = &*get_global_config()
        .expect("All test env vars are set in .cargo/config.toml");

    assert_eq!(alarms_db.host_addr, "test host");
    assert_eq!(alarms_kafka.host_addr, "test host");
    assert_eq!(alarms_kafka.topic, "test topic");
    assert_eq!(alarms_svc.host_addr, "test host");
    assert_eq!(clock.host_addr, "test host");
    assert_eq!(devdb.host_addr, "test host");
    assert_eq!(tlg.host_addr, "test host");
    assert_eq!(unr.host_addr, "test host");
    assert_eq!(wscan.host_addr, "test host");
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
