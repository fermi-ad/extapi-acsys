use crate::g_rpc::tlg;
use async_graphql::*;
use tracing::error;

// Pull in our local types.

pub mod types;

#[derive(Default)]
pub struct TlgQueries;

#[Object]
impl TlgQueries {
    #[doc = "Returns the version of the TLG service"]
    async fn get_version(&self) -> String {
        tlg::get_version().await.unwrap()
    }
}

#[derive(Default)]
pub struct TlgMutations;

#[Object]
impl TlgMutations {
    #[doc = "Returns the diagnostics of the requested devices"]
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

    #[doc = "Returns the placement of the requested devices"]
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
