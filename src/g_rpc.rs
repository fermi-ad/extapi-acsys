//! gRPC Module
//!
//! This module holds all the gRPC protocols. They all get built and
//! added to this module so that messages common to several services
//! are only generated and compiled once.

mod errors;

pub mod alarms_db;
pub mod alarms_svc;
pub mod clock;
pub mod devdb;
pub mod dpm;
pub mod proto;
pub mod tlg;
pub mod unr;
pub mod wscan;
