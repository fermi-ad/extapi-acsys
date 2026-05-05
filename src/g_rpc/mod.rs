//! gRPC Module
//!
//! This module holds all the gRPC protocols. They all get built and
//! added to this module so that messages common to several services
//! are only generated and compiled once.

pub mod proto {
    pub mod common {
        pub mod status {
            include!("generated/common.status.rs");
        }
        pub mod device {
            include!("generated/common.device.rs");
        }
    }

    pub mod services {
        pub mod aclk {
            include!("generated/services.clock_event.rs");
        }
	#[cfg(feature = "alarms")]
        pub mod alarms {
            include!("generated/services.alarm_commands.rs");
            include!("generated/services.alarm_groups.rs");
            include!("generated/services.alarm_timers.rs");
            include!("generated/services.alarm_user_layouts.rs");
        }
        pub mod daq {
            include!("generated/services.daq.rs");
        }
        pub mod devdb {
            include!("generated/services.devdb.rs");
        }
    }
}

#[cfg(feature = "alarms")]
pub mod alarms_db;
#[cfg(feature = "alarms")]
pub mod alarms_svc;
pub mod clock;
pub mod devdb;
pub mod dpm;
pub mod tlg;
pub mod wscan;

mod connection_utils;
