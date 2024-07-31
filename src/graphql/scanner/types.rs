use async_graphql::{types, SimpleObject};
use std::collections::HashMap;

#[derive(SimpleObject)]
pub struct ScanProgress {
    pub message: String,
    pub detector_id: types::ID,
    pub start_time: Option<i32>,
    pub current_position: Option<f32>,
    pub progress_percentage: Option<i32>,
}

#[derive(SimpleObject)]
pub struct ScanResult {
    pub progress: ScanProgress,
    pub voltage: Vec<f32>,
}

#[derive(SimpleObject)]
pub struct KnownStations {
    pub map: HashMap<String, String>,
}
