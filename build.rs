fn main() -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .protoc_arg("--experimental_allow_proto3_optional")
	    .emit_rerun_if_changed(true)
        .compile_well_known_types(true)
        .type_attribute(".google.protobuf.Timestamp", "#[derive(serde::Deserialize)]")
        .type_attribute(".common.alarm", "#[derive(serde::Deserialize)]")
        .enum_attribute(".common.alarm", "#[derive(async_graphql::Enum)]")
        .compile_protos(
            &[
                "src/g_rpc/protos/proto/controls/common/v1/alarm.proto",
                "src/g_rpc/protos/proto/controls/common/v1/status.proto",
                "src/g_rpc/protos/proto/controls/service/ACLK/v1/ACLK.proto",
                "src/g_rpc/protos/proto/controls/service/DAQ/v1/DAQ.proto",
                "src/g_rpc/protos/proto/controls/service/DevDB/v1/DevDB.proto",
                "src/g_rpc/protos/proto/controls/service/grpc-alarm-commands/v1/alarm_commands.proto",
                "src/g_rpc/protos/proto/controls/service/grpc-alarms-db/v1/alarm-groups.proto",
                "src/g_rpc/protos/proto/controls/service/grpc-alarms-db/v1/alarm-timers.proto",
                "src/g_rpc/protos/proto/controls/service/grpc-alarms-db/v1/user-layouts.proto",
		"src/g_rpc/protos/proto/controls/service/TlgPlacement/v1/TlgPlacement.proto",
                "src/g_rpc/protos/proto/controls/third-party/interval.proto",
            ],
            &["src/g_rpc/protos"],
        )?;

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .emit_rerun_if_changed(true)
        .compile_protos(&["src/g_rpc/wscan/WScan.proto"], &[])?;

    Ok(())
}
