use tower_http::compression::CompressionLayer;

/// Returns a [`CompressionLayer`] that compresses responses with Zstd and Gzip.
pub fn compression_layer() -> CompressionLayer {
    CompressionLayer::new().zstd(true).gzip(true)
}
