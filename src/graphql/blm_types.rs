use async_graphql::{Enum, ID, SimpleObject};
use chrono::{DateTime, Utc};

use crate::g_rpc::proto::{
    common::status::Status,
    services::blm::v1::{
        BeamEfficiencies, BeamLine, DeviceData, DeviceLossRatio,
        DeviceMetadata, SubscribeBeamThroughputResponse,
        SubscribeLossRatioResponse,
    },
};

/// Beamlines defined by the BLM protobuf API.
#[derive(Clone, Copy, Debug, Eq, Enum, PartialEq)]
pub enum BlmBeamLine {
    #[graphql(name = "BEAM_LINE_UNSPECIFIED")]
    Unspecified,
    #[graphql(name = "BEAM_LINE_LINAC")]
    Linac,
    #[graphql(name = "BEAM_LINE_LINAC2")]
    Linac2,
    #[graphql(name = "BEAM_LINE_400MEV")]
    BeamLine400mev,
    #[graphql(name = "BEAM_LINE_BTL")]
    Btl,
    #[graphql(name = "BEAM_LINE_BOOSTER")]
    Booster,
    #[graphql(name = "BEAM_LINE_8GEV")]
    BeamLine8gev,
    #[graphql(name = "BEAM_LINE_MAIN_INJECTOR")]
    MainInjector,
    #[graphql(name = "BEAM_LINE_RECYCLER")]
    Recycler,
    #[graphql(name = "BEAM_LINE_P1")]
    P1,
    #[graphql(name = "BEAM_LINE_P2")]
    P2,
    #[graphql(name = "BEAM_LINE_P3")]
    P3,
    #[graphql(name = "BEAM_LINE_M1")]
    M1,
    #[graphql(name = "BEAM_LINE_M2")]
    M2,
    #[graphql(name = "BEAM_LINE_M3")]
    M3,
    #[graphql(name = "BEAM_LINE_M4")]
    M4,
    #[graphql(name = "BEAM_LINE_M5")]
    M5,
    #[graphql(name = "BEAM_LINE_DELIVERY_RING")]
    DeliveryRing,
}

impl BlmBeamLine {
    pub const fn proto_value(self) -> i32 {
        match self {
            Self::Unspecified => BeamLine::Unspecified as i32,
            Self::Linac => BeamLine::Linac as i32,
            Self::Linac2 => BeamLine::Linac2 as i32,
            Self::BeamLine400mev => BeamLine::BeamLine400mev as i32,
            Self::Btl => BeamLine::Btl as i32,
            Self::Booster => BeamLine::Booster as i32,
            Self::BeamLine8gev => BeamLine::BeamLine8gev as i32,
            Self::MainInjector => BeamLine::MainInjector as i32,
            Self::Recycler => BeamLine::Recycler as i32,
            Self::P1 => BeamLine::P1 as i32,
            Self::P2 => BeamLine::P2 as i32,
            Self::P3 => BeamLine::P3 as i32,
            Self::M1 => BeamLine::M1 as i32,
            Self::M2 => BeamLine::M2 as i32,
            Self::M3 => BeamLine::M3 as i32,
            Self::M4 => BeamLine::M4 as i32,
            Self::M5 => BeamLine::M5 as i32,
            Self::DeliveryRing => BeamLine::DeliveryRing as i32,
        }
    }

    pub fn from_proto_value(value: i32) -> Option<Self> {
        match BeamLine::try_from(value).ok()? {
            BeamLine::Unspecified => Some(Self::Unspecified),
            BeamLine::Linac => Some(Self::Linac),
            BeamLine::Linac2 => Some(Self::Linac2),
            BeamLine::BeamLine400mev => Some(Self::BeamLine400mev),
            BeamLine::Btl => Some(Self::Btl),
            BeamLine::Booster => Some(Self::Booster),
            BeamLine::BeamLine8gev => Some(Self::BeamLine8gev),
            BeamLine::MainInjector => Some(Self::MainInjector),
            BeamLine::Recycler => Some(Self::Recycler),
            BeamLine::P1 => Some(Self::P1),
            BeamLine::P2 => Some(Self::P2),
            BeamLine::P3 => Some(Self::P3),
            BeamLine::M1 => Some(Self::M1),
            BeamLine::M2 => Some(Self::M2),
            BeamLine::M3 => Some(Self::M3),
            BeamLine::M4 => Some(Self::M4),
            BeamLine::M5 => Some(Self::M5),
            BeamLine::DeliveryRing => Some(Self::DeliveryRing),
        }
    }
}

#[derive(SimpleObject)]
pub struct BlmDeviceMetadata {
    pub device_plot_label: String,
}

impl From<DeviceMetadata> for BlmDeviceMetadata {
    fn from(value: DeviceMetadata) -> Self {
        Self {
            device_plot_label: value.device_plot_label,
        }
    }
}

#[derive(SimpleObject)]
pub struct BlmDevice {
    pub device: ID,
    pub metadata: Option<BlmDeviceMetadata>,
}

impl From<DeviceData> for BlmDevice {
    fn from(value: DeviceData) -> Self {
        Self {
            device: ID(value.device),
            metadata: value.metadata.map(Into::into),
        }
    }
}

#[derive(SimpleObject)]
pub struct BlmStatus {
    pub facility_code: i32,
    pub status_code: i32,
    pub message: String,
}

impl From<Status> for BlmStatus {
    fn from(value: Status) -> Self {
        Self {
            facility_code: value.facility_code,
            status_code: value.status_code,
            message: value.message,
        }
    }
}

#[derive(SimpleObject)]
pub struct BlmDeviceLossRatio {
    pub device_data: Option<BlmDevice>,
    pub integrated_loss: i32,
    pub loss_limit: i32,
    pub ratio: f64,
    pub timestamp: Option<DateTime<Utc>>,
    pub status: Option<BlmStatus>,
}

impl From<DeviceLossRatio> for BlmDeviceLossRatio {
    fn from(value: DeviceLossRatio) -> Self {
        Self {
            device_data: value.device_data.map(Into::into),
            integrated_loss: value.integrated_loss,
            loss_limit: value.loss_limit,
            ratio: value.ratio,
            timestamp: value.timestamp.and_then(|timestamp| {
                DateTime::from_timestamp(
                    timestamp.seconds,
                    timestamp.nanos as u32,
                )
            }),
            status: value.status.map(Into::into),
        }
    }
}

#[derive(SimpleObject)]
pub struct BlmLossRatioSample {
    pub ratios: Vec<BlmDeviceLossRatio>,
}

impl From<SubscribeLossRatioResponse> for BlmLossRatioSample {
    fn from(value: SubscribeLossRatioResponse) -> Self {
        Self {
            ratios: value.ratios.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(SimpleObject)]
pub struct BlmBeamEfficiency {
    pub tclk_event: u32,
    pub efficiency: u32,
    pub protons_per_hour: u32,
}

impl From<BeamEfficiencies> for BlmBeamEfficiency {
    fn from(value: BeamEfficiencies) -> Self {
        Self {
            tclk_event: value.tclk_event,
            efficiency: value.efficiency,
            protons_per_hour: value.protons_per_hour,
        }
    }
}

#[derive(SimpleObject)]
pub struct BlmBeamThroughputSample {
    pub beam_line: Option<BlmBeamLine>,
    pub protons_per_pulse: u32,
    pub protons_per_hour: u32,
    pub protons_per_hour_limit: u32,
    pub efficiencies: Vec<BlmBeamEfficiency>,
}

impl From<SubscribeBeamThroughputResponse> for BlmBeamThroughputSample {
    fn from(value: SubscribeBeamThroughputResponse) -> Self {
        Self {
            beam_line: BlmBeamLine::from_proto_value(value.beam_line),
            protons_per_pulse: value.protons_per_pulse,
            protons_per_hour: value.protons_per_hour,
            protons_per_hour_limit: value.protons_per_hour_limit,
            efficiencies: value
                .efficiencies
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}
