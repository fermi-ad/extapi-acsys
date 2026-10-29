use async_graphql::{Enum, SimpleObject};

use crate::g_rpc::proto::services::mi8_autotune::v1 as proto;

/// Unrecognized reports map to UNKNOWN; UNKNOWN is rejected as mutation input.
#[derive(Enum, Copy, Clone, Debug, Eq, PartialEq)]
pub enum ArmedState {
    Unknown,
    Armed,
    Disarmed,
}

impl From<i32> for ArmedState {
    fn from(value: i32) -> Self {
        match proto::ArmedState::try_from(value) {
            Ok(proto::ArmedState::Armed) => Self::Armed,
            Ok(proto::ArmedState::Disarmed) => Self::Disarmed,
            Ok(proto::ArmedState::Unknown) | Err(_) => Self::Unknown,
        }
    }
}

/// Reported or requested mode; UNKNOWN is rejected as mutation input.
#[derive(Enum, Copy, Clone, Debug, Eq, PartialEq)]
pub enum OperatingMode {
    Unknown,
    Live,
    Shadow,
}

impl From<i32> for OperatingMode {
    fn from(value: i32) -> Self {
        match proto::OperatingMode::try_from(value) {
            Ok(proto::OperatingMode::Live) => Self::Live,
            Ok(proto::OperatingMode::Shadow) => Self::Shadow,
            Ok(proto::OperatingMode::Unknown) | Err(_) => Self::Unknown,
        }
    }
}

#[derive(Enum, Copy, Clone, Debug, Eq, PartialEq)]
pub enum AutotuneEvent {
    Unknown,
    NoAuto,
    FirstPass,
    Move,
    MovePending,
    DaqError,
    BigMove,
    VernierMove,
    BeamOk,
    BigSetting,
    IntensityLow,
    AuxiliaryBad,
    SettingError,
}

impl From<i32> for AutotuneEvent {
    fn from(value: i32) -> Self {
        match proto::AutotuneEvent::try_from(value) {
            Ok(proto::AutotuneEvent::NoAuto) => Self::NoAuto,
            Ok(proto::AutotuneEvent::FirstPass) => Self::FirstPass,
            Ok(proto::AutotuneEvent::Move) => Self::Move,
            Ok(proto::AutotuneEvent::MovePending) => Self::MovePending,
            Ok(proto::AutotuneEvent::DaqError) => Self::DaqError,
            Ok(proto::AutotuneEvent::BigMove) => Self::BigMove,
            Ok(proto::AutotuneEvent::VernierMove) => Self::VernierMove,
            Ok(proto::AutotuneEvent::BeamOk) => Self::BeamOk,
            Ok(proto::AutotuneEvent::BigSetting) => Self::BigSetting,
            Ok(proto::AutotuneEvent::IntensityLow) => Self::IntensityLow,
            Ok(proto::AutotuneEvent::AuxiliaryBad) => Self::AuxiliaryBad,
            Ok(proto::AutotuneEvent::SettingError) => Self::SettingError,
            Ok(proto::AutotuneEvent::Unknown) | Err(_) => Self::Unknown,
        }
    }
}

/// An upstream event and its associated message, not a transport error.
#[derive(SimpleObject, Debug, Eq, PartialEq)]
pub struct AutotuneEventUpdate {
    pub autotune_event: AutotuneEvent,
    pub error_message: String,
}

impl From<proto::SubscribeAutotuneEventResponse> for AutotuneEventUpdate {
    fn from(response: proto::SubscribeAutotuneEventResponse) -> Self {
        Self {
            autotune_event: response.autotune_event.into(),
            error_message: response.error_message,
        }
    }
}
