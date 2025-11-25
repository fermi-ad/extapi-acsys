// This module implements the client side of the wire scan gRPC
// protocol.

use proto::{
    scanner_client::ScannerClient, DetectorRequest, ScanProgress, ScanRequest,
    ScanResult,
};
use std::collections::HashMap;
use tonic::{transport, Response, Status, Streaming};

pub mod proto {
    tonic::include_proto!("scanner");
}

use crate::env_var;

const WIRE_SCANNER_HOST: &str = "SCANNER_GRPC_HOST";
const DEFAULT_WIRE_SCANNER_HOST: &str = "unknown.fnal.gov";

const WIRE_SCANNER_PORT: &str = "SCANNER_GRPC_PORT";
const DEFAULT_WIRE_SCANNER_PORT: &str = "50051";

// Local helper function to get a connection to the gRPC service.

async fn get_client() -> Result<ScannerClient<transport::Channel>, Status> {
    let host =
        env_var::get(WIRE_SCANNER_HOST).as_str_or(DEFAULT_WIRE_SCANNER_HOST);
    let port =
        env_var::get(WIRE_SCANNER_PORT).as_str_or(DEFAULT_WIRE_SCANNER_PORT);
    let address = format!("http://{}:{}", host, port);
    ScannerClient::connect(address)
        .await
        .map_err(|_| Status::unavailable("wire-scanner service unavailable"))
}

pub async fn _retrieve_scans() -> Result<HashMap<String, String>, Status> {
    let map = HashMap::from([
        (
            "scl-ws-station1".into(),
            "Super Conduction Linac Wire Scanner - Station 1".into(),
        ),
        (
            "scl-ws-station2".into(),
            "Super Conducting Linac Wire Scanner - Station 2".into(),
        ),
    ]);

    Ok(map)
}

pub async fn start_scan(
    id: String, pos_start: f32, pos_end: f32, pos_step: f32, samp_dur: f32,
    pps: i32,
) -> Result<Response<Streaming<ScanResult>>, Status> {
    get_client()
        .await?
        .start_scan(ScanRequest {
            detector_id: id,
            position_start: pos_start,
            position_end: pos_end,
            position_step: pos_step,
            sampling_duration: samp_dur,
            pulses_per_sample: pps,
        })
        .await
}

pub async fn get_progress(
    id: String,
) -> Result<Response<ScanProgress>, Status> {
    get_client()
        .await?
        .get_progress(DetectorRequest { detector_id: id })
        .await
}

pub async fn abort_scan(id: String) -> Result<Response<ScanProgress>, Status> {
    get_client()
        .await?
        .abort_scan(DetectorRequest { detector_id: id })
        .await
}
