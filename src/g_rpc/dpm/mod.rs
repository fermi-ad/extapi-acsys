use super::proto::{
    common::device,
    services::daq::{
        ReadingList, ReadingReply, Setting, SettingList, SettingReply,
        daq_client::DaqClient,
    },
};
use tokio::time::{Duration, timeout};
use tonic::transport::{Channel, Endpoint, Error};
use tracing::{error, info, instrument, warn};

pub struct Connection(DaqClient<Channel>);

type TonicStreamResult<T> =
    Result<tonic::Response<tonic::Streaming<T>>, tonic::Status>;
type _TonicQueryResult<T> = Result<T, tonic::Status>;

// Builds a sharable connection to the DPM pool. All instances will use the
// same connection.

pub async fn build_connection() -> Result<Connection, Error> {
    const HOSTS: [&str; 14] = [
        "http://dce01:50051",
        "http://dce02:50051",
        "http://dce03:50051",
        "http://dce04:50051",
        "http://dce05:50051",
        "http://dce06:50051",
        "http://dce07:50051",
        "http://dce08:50051",
        "http://dce09:50051",
        "http://dce10:50051",
        "http://dce11:50051",
        "http://dce12:50051",
        "http://dce13:50051",
        "http://dce14:50051",
    ];

    let endpoints = HOSTS.iter().map(|h| Endpoint::from_static(h));
    let channel = Channel::balance_list(endpoints);

    Ok(Connection(DaqClient::new(channel)))
}

#[instrument(skip(conn, jwt, devices))]
pub async fn acquire_devices(
    conn: &Connection, jwt: Option<&String>, devices: Vec<String>,
) -> TonicStreamResult<ReadingReply> {
    info!("requesting {:?}", &devices);

    let mut req = tonic::Request::new(ReadingList { drf: devices });

    if let Some(jwt) = jwt {
        use std::str::FromStr;
        use tonic::metadata::MetadataValue;

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

    match timeout(Duration::from_secs(10), conn.0.clone().read(req)).await {
        Ok(response) => {
            if let Err(ref e) = response {
                error!("error creating stream : {}", &e)
            }
            response
        }
        Err(_) => {
            error!("connection to DPM timed-out");
            Err(tonic::Status::cancelled("connection to DPM timed-out"))
        }
    }
}

// This function wraps the logic needed to make the `ApplySettings()`
// gRPC transaction.

pub async fn set_device(
    conn: &Connection, session_id: Option<String>, device: String,
    value: device::Value,
) -> _TonicQueryResult<Vec<i32>> {
    use tonic::{IntoRequest, metadata::MetadataValue};

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
            }
            Err(err) => {
                error!("unable to pass credentials : {}", &err);
                return Err(tonic::Status::internal(
                    "couldn't add credentials",
                ));
            }
        }
    } else {
	warn!("request lacks credentials ... setting has been blocked");
    }

    let SettingReply { status } = conn.0.clone().set(req).await?.into_inner();

    Ok(status
        .iter()
        .map(|v| v.facility_code + v.status_code * 256)
        .collect())
}
