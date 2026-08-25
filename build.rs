use rust_grpc_lib::build_support::{Config, generate_protos};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::new()
        .type_attribute(
            ".google.protobuf.Timestamp",
            "#[derive(serde::Deserialize)]",
        )
        .type_attribute(".common.alarm", "#[derive(serde::Deserialize)]")
        .enum_attribute(".common.alarm", "#[derive(async_graphql::Enum)]");

    generate_protos(config)?;

    Ok(())
}
