use super::proto::{
    common::device,
    services::daq::{
        ReadingList, ReadingReply, Setting, SettingList, SettingReply,
        daq_client::DaqClient,
    },
};
use crate::env_var;
use std::str::FromStr;
use tokio::time::{Duration, timeout};
use tonic::{
    IntoRequest, Request, Response, Status, Streaming,
    metadata::MetadataValue,
    transport::{Channel, Error},
};
use tracing::{error, info, instrument, warn};

type TonicStreamResult<T> = Result<Response<Streaming<T>>, Status>;
type TonicQueryResult<T> = Result<T, Status>;

pub struct Connection {
    daq_client: DaqClient<Channel>,
}

const DPM_HOST: &str = "DPM_GRPC_HOST";
/// Builds a sharable connection to the DPM pool. All instances will use the
/// same connection.
pub async fn build_connection() -> Result<Connection, Error> {
    let host: String = env_var::expect(DPM_HOST);

    Ok(Connection {
        daq_client: DaqClient::connect(host).await?,
    })
}

#[instrument(skip(conn, jwt, devices))]
pub async fn acquire_devices(
    conn: &Connection, jwt: Option<&String>, devices: Vec<String>,
) -> TonicStreamResult<ReadingReply> {
    info!("requesting {:?}", &devices);

    let mut req = Request::new(ReadingList { drf: devices });

    if let Some(jwt) = jwt {
        match MetadataValue::from_str(&format!("Bearer {}", jwt)) {
            Ok(val) => {
                req.metadata_mut().insert("authorization", val);
            }
            Err(e) => warn!("error creating JWT : {}", e),
        }
    }

    // XXX: This 10 second timeout is excessive. While we learn more about
    // GraphQL and gRPCs, we stretched this so that we're not competing
    // with DPM's timeouts.

    match timeout(Duration::from_secs(10), conn.daq_client.clone().read(req))
        .await
    {
        Ok(response) => {
            if let Err(ref e) = response {
                error!("error creating stream : {}", &e)
            }
            response
        }
        Err(_) => {
            error!("connection to DPM timed-out");
            Err(Status::cancelled("connection to DPM timed-out"))
        }
    }
}

// This function wraps the logic needed to make the `ApplySettings()`
// gRPC transaction.

pub async fn set_device(
    conn: &Connection, session_id: Option<String>, device: String,
    value: device::Value,
) -> TonicQueryResult<Vec<i32>> {
    info!("setting to {:?}", &value);

    // Build the setting request. This function only sets one device, so the
    // request only has a 1-element array containing the setting.

    let mut req = SettingList {
        setting: vec![Setting {
            device,
            value: Some(value),
        }],
    }
    .into_request();

    // If a JWT token has been found, add it to the request.

    if let Some(token) = session_id {
        match MetadataValue::try_from(format!("Bearer {token}")) {
            Ok(val) => {
                req.metadata_mut().insert("authorization", val);

                let SettingReply { status } =
                    conn.daq_client.clone().set(req).await?.into_inner();

                Ok(status
                    .iter()
                    .map(|v| v.facility_code + v.status_code * 256)
                    .collect())
            }
            Err(err) => {
                error!("unable to pass credentials : {}", &err);
                Err(Status::internal("couldn't add credentials"))
            }
        }
    } else {
        Err(Status::internal("not authorized"))
    }
}
