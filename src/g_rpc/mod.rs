// This module holds all the gRPC protocols. They all get built and
// added to this module so that messages common to several services
// are only generated and compiled once.

pub mod proto {
    pub mod third_party {
        pub mod r#type {
            include!("generated/third_party.r#type.rs");
        }
    }

    pub mod common {
        pub mod status {
            include!("generated/common.status.rs");
        }
        pub mod event {
            include!("generated/common.event.rs");
        }
        pub mod device {
            include!("generated/common.device.rs");
        }
        pub mod sources {
            include!("generated/common.sources.rs");
        }
        pub mod drf {
            include!("generated/common.drf.rs");
        }
    }

    pub mod services {
        pub mod daq {
            include!("generated/services.daq.rs");
        }
        pub mod aclk {
            include!("generated/services.clock_event.rs");
        }
    }
}

pub mod clock;
pub mod devdb;
pub mod dpm;
pub mod tlg;
pub mod wscan;
pub mod xform;
