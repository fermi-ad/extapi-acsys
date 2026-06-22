use super::*;
use crate::g_rpc::proto::{
    google::protobuf::Timestamp, services::alarms::TimerType,
};

#[test]
fn alarm_from_proto_to_gql() {
    let status = Status {
        device: "M:BEAM".to_string(),
        source: Source::Analog as i32,
        state: State::Ok as i32,
        severity: Severity::Unknown as i32,
        acknowledgeable: false,
        time: Some(Timestamp {
            seconds: 1_234_567_890,
            nanos: 0,
        }),
        epics_type: String::new(),
        user: "test user".to_string(),
        wake: None,
    };

    let output = Alarm::from(status.clone());

    assert_eq!(status.device, output.device);
    assert_eq!(status.source(), output.source);
    assert_eq!(status.state(), output.state);
    assert_eq!(status.severity(), output.severity);
    assert_eq!(status.acknowledgeable, output.acknowledgeable);
    assert_eq!(utils::timestamp_to_datetime(status.time), output.time);
    assert_eq!(status.epics_type, output.epics_type);
    assert_eq!(status.user, output.user);
    assert_eq!(utils::timestamp_to_datetime(status.wake), output.wake);
}

#[test]
fn alarm_try_from_message() {
    let source_int = Source::Analog as i32;
    let state_int = State::Ok as i32;
    let severity_int = Severity::Unknown as i32;
    let text = format!(
        r#"
            {{
                "device": "M:BEAM",
                "source": {source_int},
                "state": {state_int},
                "severity": {severity_int},
                "acknowledgeable": false,
                "epics_type": "",
                "user": ""
            }}
        "#
    );
    let message = Message::new(None, text.to_string());

    let output = Alarm::try_from(message).unwrap();
    assert_eq!(output.device, "M:BEAM");
    assert_eq!(output.source, Source::Analog);
    assert_eq!(output.state, State::Ok);
    assert_eq!(output.severity, Severity::Unknown);
    assert!(!output.acknowledgeable);
    assert_eq!(output.time, None);
    assert_eq!(output.epics_type, "");
    assert_eq!(output.user, "");
    assert_eq!(output.wake, None);
}

#[test]
fn group_from_proto_to_gql() {
    let test_name = "device A";
    let test_desc = "desc";
    let test_seconds = 100;
    let test_nanos = 3242;
    let test_user = "user 1";
    let proto_meta = ProtoGroupMetadatum {
        name: test_name.to_string(),
        description: test_desc.to_string(),
        is_user_category: true,
        updated_at: Some(Timestamp {
            seconds: test_seconds,
            nanos: test_nanos,
        }),
        updated_by: test_user.to_string(),
    };
    let test_devices = vec![String::from("device 1"), String::from("device 2")];
    let test_groups = vec![String::from("group 1")];
    let proto = ProtoAlarmGroup {
        metadata: Some(proto_meta.clone()),
        devices: test_devices.clone(),
        groups: test_groups.clone(),
    };

    let result = AlarmGroup::from(proto);

    assert_eq!(result.devices, test_devices);
    assert_eq!(result.groups, test_groups);
    assert_eq!(
        result.metadata.unwrap(),
        AlarmGroupMetadatum::from(proto_meta)
    );
}

#[test]
fn metadatum_from_proto_to_gql() {
    let test_name = "device A";
    let test_desc = "desc";
    let test_seconds = 100;
    let test_nanos = 3242;
    let test_user = "user 1";
    let proto = ProtoGroupMetadatum {
        name: test_name.to_string(),
        description: test_desc.to_string(),
        is_user_category: true,
        updated_at: Some(Timestamp {
            seconds: test_seconds,
            nanos: test_nanos,
        }),
        updated_by: test_user.to_string(),
    };

    let result = AlarmGroupMetadatum::from(proto);

    assert_eq!(result.name, test_name);
    assert_eq!(result.description, test_desc);
    assert!(result.is_user_category);
    assert_eq!(
        result.updated_at,
        DateTime::from_timestamp(test_seconds, test_nanos as u32)
    );
    assert_eq!(result.updated_by, test_user);
}

#[test]
fn timer_from_proto_to_gql() {
    let test_device = "device A";
    let test_seconds = 100;
    let test_nanos = 3242;
    let test_user = "user 1";
    let proto = ProtoAlarmTimer {
        device: test_device.to_string(),
        end_time: Some(Timestamp {
            seconds: test_seconds,
            nanos: test_nanos,
        }),
        timer_type: TimerType::BypassReminder as i32,
        updated_at: None,
        updated_by: test_user.to_string(),
    };

    let result = AlarmTimer::from(proto);

    assert_eq!(result.device, test_device);
    assert_eq!(
        result.end_time,
        DateTime::from_timestamp(test_seconds, test_nanos as u32)
    );
    assert_eq!(result.timer_type, TimerType::BypassReminder.as_str_name());
    assert_eq!(result.updated_at, None);
    assert_eq!(result.updated_by, test_user);
}

#[test]
fn user_layout_from_proto_to_gql() {
    let test_user = "user1";
    let grp1 = "group 1";
    let grp2 = "group 2";
    let test_groups = vec![grp1.to_string(), grp2.to_string()];
    let proto_layout = ProtoUserLayout {
        user_name: test_user.to_string(),
        groups: test_groups.clone(),
    };

    let result = UserLayout::from(proto_layout);

    assert_eq!(result.user_name, test_user);
    assert_eq!(result.groups, test_groups);
}
