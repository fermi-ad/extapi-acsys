use rust_grpc_lib::build_support::{Config, generate_protos};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::new()
        .type_attribute(
            ".google.protobuf.Timestamp",
            "#[derive(serde::Deserialize)]",
        )
        .type_attribute(".common.alarm", "#[derive(serde::Deserialize)]")
        .enum_attribute(".common.alarm", "#[derive(async_graphql::Enum)]")
        .enum_attribute(
            ".services.blm.v1.BeamLine",
            "#[derive(async_graphql::Enum)]",
        )
        .field_attribute(
            ".services.blm.v1.BeamLine.BEAM_LINE_UNSPECIFIED",
            "#[graphql(name = \"BEAM_LINE_UNSPECIFIED\")]",
        )
        .field_attribute(
            ".services.blm.v1.BeamLine.BEAM_LINE_LINAC",
            "#[graphql(name = \"BEAM_LINE_LINAC\")]",
        )
        .field_attribute(
            ".services.blm.v1.BeamLine.BEAM_LINE_LINAC2",
            "#[graphql(name = \"BEAM_LINE_LINAC2\")]",
        )
        .field_attribute(
            ".services.blm.v1.BeamLine.BEAM_LINE_400MEV",
            "#[graphql(name = \"BEAM_LINE_400MEV\")]",
        )
        .field_attribute(
            ".services.blm.v1.BeamLine.BEAM_LINE_BTL",
            "#[graphql(name = \"BEAM_LINE_BTL\")]",
        )
        .field_attribute(
            ".services.blm.v1.BeamLine.BEAM_LINE_BOOSTER",
            "#[graphql(name = \"BEAM_LINE_BOOSTER\")]",
        )
        .field_attribute(
            ".services.blm.v1.BeamLine.BEAM_LINE_8GEV",
            "#[graphql(name = \"BEAM_LINE_8GEV\")]",
        )
        .field_attribute(
            ".services.blm.v1.BeamLine.BEAM_LINE_MAIN_INJECTOR",
            "#[graphql(name = \"BEAM_LINE_MAIN_INJECTOR\")]",
        )
        .field_attribute(
            ".services.blm.v1.BeamLine.BEAM_LINE_RECYCLER",
            "#[graphql(name = \"BEAM_LINE_RECYCLER\")]",
        )
        .field_attribute(
            ".services.blm.v1.BeamLine.BEAM_LINE_P1",
            "#[graphql(name = \"BEAM_LINE_P1\")]",
        )
        .field_attribute(
            ".services.blm.v1.BeamLine.BEAM_LINE_P2",
            "#[graphql(name = \"BEAM_LINE_P2\")]",
        )
        .field_attribute(
            ".services.blm.v1.BeamLine.BEAM_LINE_P3",
            "#[graphql(name = \"BEAM_LINE_P3\")]",
        )
        .field_attribute(
            ".services.blm.v1.BeamLine.BEAM_LINE_M1",
            "#[graphql(name = \"BEAM_LINE_M1\")]",
        )
        .field_attribute(
            ".services.blm.v1.BeamLine.BEAM_LINE_M2",
            "#[graphql(name = \"BEAM_LINE_M2\")]",
        )
        .field_attribute(
            ".services.blm.v1.BeamLine.BEAM_LINE_M3",
            "#[graphql(name = \"BEAM_LINE_M3\")]",
        )
        .field_attribute(
            ".services.blm.v1.BeamLine.BEAM_LINE_M4",
            "#[graphql(name = \"BEAM_LINE_M4\")]",
        )
        .field_attribute(
            ".services.blm.v1.BeamLine.BEAM_LINE_M5",
            "#[graphql(name = \"BEAM_LINE_M5\")]",
        )
        .field_attribute(
            ".services.blm.v1.BeamLine.BEAM_LINE_DELIVERY_RING",
            "#[graphql(name = \"BEAM_LINE_DELIVERY_RING\")]",
        );

    generate_protos(config)?;

    Ok(())
}
