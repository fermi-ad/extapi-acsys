fn main() -> Result<(), Box<dyn std::error::Error>> {
    let incl: [&str; 0] = [];
    unsafe {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("src/g_rpc/generated")
	.emit_rerun_if_changed(true)
        .compile_protos(
            &[
                "src/g_rpc/protos/proto/controls/service/DAQ/v1/DAQ.proto",
                "src/g_rpc/protos/proto/controls/service/ACLK/v1/ACLK.proto",
		"src/g_rpc/protos/proto/controls/service/TlgPlacement/v1/TlgPlacement.proto",
                "src/g_rpc/protos/proto/controls/common/v1/status.proto",
                "src/g_rpc/protos/proto/controls/third-party/interval.proto",
            ],
            &["src/g_rpc/protos"],
        )?;

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
	.emit_rerun_if_changed(true)
        .compile_protos(&["src/g_rpc/xform/XForm.proto"], &incl)?;

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
	.emit_rerun_if_changed(true)
        .compile_protos(&["src/g_rpc/wscan/WScan.proto"], &incl)?;

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .protoc_arg("--experimental_allow_proto3_optional")
        .type_attribute(
            ".devdb.InfoEntry.result",
            "#[allow(clippy::large_enum_variant)]",
        )
        .compile_protos(&["src/g_rpc/devdb/DevDB.proto"], &incl)?;

    Ok(())
}
