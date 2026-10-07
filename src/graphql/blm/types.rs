use async_graphql::SimpleObject;
use chrono::{DateTime, Utc};

use crate::g_rpc::proto::{
    common::status::Status,
    services::blm::v1::{
        BeamEfficiencies, BeamLine, DeviceData, DeviceLossRatio,
        DeviceMetadata, SubscribeBeamThroughputResponse,
        SubscribeLossRatioResponse,
    },
};

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
    pub device: String,
    pub metadata: Option<BlmDeviceMetadata>,
}

impl From<DeviceData> for BlmDevice {
    fn from(value: DeviceData) -> Self {
        Self {
            device: value.device,
            metadata: value.metadata.map(BlmDeviceMetadata::from),
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
            device_data: value.device_data.map(BlmDevice::from),
            integrated_loss: value.integrated_loss,
            loss_limit: value.loss_limit,
            ratio: value.ratio,
            timestamp: value.timestamp.and_then(|timestamp| {
                DateTime::from_timestamp(
                    timestamp.seconds,
                    timestamp.nanos as u32,
                )
            }),
            status: value.status.map(BlmStatus::from),
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
            ratios: value
                .ratios
                .into_iter()
                .map(BlmDeviceLossRatio::from)
                .collect(),
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
    pub beam_line: Option<BeamLine>,
    pub protons_per_pulse: u32,
    pub protons_per_hour: u32,
    pub protons_per_hour_limit: u32,
    pub efficiencies: Vec<BlmBeamEfficiency>,
}

impl From<SubscribeBeamThroughputResponse> for BlmBeamThroughputSample {
    fn from(value: SubscribeBeamThroughputResponse) -> Self {
        Self {
            beam_line: BeamLine::try_from(value.beam_line).ok(),
            protons_per_pulse: value.protons_per_pulse,
            protons_per_hour: value.protons_per_hour,
            protons_per_hour_limit: value.protons_per_hour_limit,
            efficiencies: value
                .efficiencies
                .into_iter()
                .map(BlmBeamEfficiency::from)
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_data_preserves_device_name_as_string() {
        let device = BlmDevice::from(DeviceData {
            device: "B:LM01".to_string(),
            metadata: Some(DeviceMetadata {
                device_plot_label: "LM01".to_string(),
            }),
        });

        assert_eq!(device.device, "B:LM01");
        assert_eq!(
            device.metadata.map(|metadata| metadata.device_plot_label),
            Some("LM01".to_string())
        );
    }

    #[test]
    fn throughput_uses_generated_beam_line() {
        let sample =
            BlmBeamThroughputSample::from(SubscribeBeamThroughputResponse {
                beam_line: BeamLine::Booster as i32,
                protons_per_pulse: 1,
                protons_per_hour: 2,
                protons_per_hour_limit: 3,
                efficiencies: Vec::new(),
            });

        assert_eq!(sample.beam_line, Some(BeamLine::Booster));
    }
}
