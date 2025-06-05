use crate::g_rpc::tlg;
use tracing::error;
use async_graphql::*;

// Pull in our local types.

pub mod types;

#[derive(Default)]
pub struct TlgQueries;

#[Object]
impl TlgQueries {
    #[doc = ""]
    async fn get_version(&self) -> String {
        tlg::get_version().await.unwrap()
    }
}

#[derive(Default)]
pub struct TlgMutations;

#[Object]
impl TlgMutations {
    #[doc = ""]
    async fn diagnostics_inline(
        &self, devices: types::TlgDevices,
    ) -> types::TlgPlacementResponse {
        match tlg::diagnostics(devices.into()).await {
            Ok(resp) => resp.into(),
            Err(e) => {
                error!("diag err -- {}", e);
                todo!()
            }
        }
    }

    #[doc = ""]
    async fn placement_inline(
        &self, devices: types::TlgDevices,
    ) -> types::TlgPlacementResponse {
        match tlg::placement(devices.into()).await {
            Ok(resp) => resp.into(),
            Err(e) => {
                error!("place err -- {}", e);
                todo!()
            }
        }
    }
}
