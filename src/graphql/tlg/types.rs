use crate::g_rpc::tlg::proto::services::tlg_placement as gRPC;
use async_graphql::*;

#[derive(InputObject)]
pub struct TlgDevice {
    pub r#type: String,
    pub name: String,
    pub device: String,
    pub data: Vec<i32>,
}
#[allow(clippy::from_over_into)]
impl Into<gRPC::TlgDevice> for TlgDevice {
    fn into(self) -> gRPC::TlgDevice {
        gRPC::TlgDevice {
            r#type: self.r#type,
            name: self.name,
            device: self.device,
            data: self.data,
        }
    }
}

#[derive(InputObject)]
pub struct TlgDevices {
    pub devices: Vec<TlgDevice>,
}

#[allow(clippy::from_over_into)]
impl Into<gRPC::TlgDevices> for TlgDevices {
    fn into(mut self) -> gRPC::TlgDevices {
        gRPC::TlgDevices {
            devices: self.devices.drain(..).map(|e| e.into()).collect(),
        }
    }
}

#[derive(SimpleObject)]
pub struct TlgPlacementResponse {
    pub status: i32,
    pub message: String,
    pub diagnostics: Vec<i32>,
    pub placement: Vec<i32>,
    pub generated: Vec<i32>,
    pub parameters: Vec<i32>,
}

impl From<gRPC::TlgPlacementResponse> for TlgPlacementResponse {
    fn from(v: gRPC::TlgPlacementResponse) -> Self {
        TlgPlacementResponse {
            status: v.status,
            message: v.message,
            diagnostics: v.diagnostics,
            placement: v.placement,
            generated: v.generated,
            parameters: v.parameters,
        }
    }
}
