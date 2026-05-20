//! gRPC Module
//!
//! This module holds all the gRPC protocols. They all get built and
//! added to this module so that messages common to several services
//! are only generated and compiled once.

pub mod proto {
    pub mod common {
        pub mod alarm {
            tonic::include_proto!("common.alarm");
        }
        pub mod status {
            tonic::include_proto!("common.status");
        }
        pub mod device {
            tonic::include_proto!("common.device");
        }
    }

    pub mod google {
        pub mod protobuf {
            tonic::include_proto!("google.protobuf");
        }
    }

    pub mod services {
        pub mod aclk {
            tonic::include_proto!("services.clock_event");
        }
        pub mod alarms {
            tonic::include_proto!("services.alarm_commands");
            tonic::include_proto!("services.alarm_groups");
            tonic::include_proto!("services.alarm_timers");
            tonic::include_proto!("services.alarm_user_layouts");
        }
        pub mod daq {
            tonic::include_proto!("services.daq");
        }
        pub mod tlg_placement {
            tonic::include_proto!("services.tlg_placement");
        }
    }
}

pub mod alarms_db;
pub mod alarms_svc;
pub mod clock;
pub mod devdb;
pub mod dpm;
pub mod tlg;
pub mod wscan;

mod connection_utils;
