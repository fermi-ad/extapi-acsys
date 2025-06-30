fn main() -> Result<(), Box<dyn std::error::Error>> {
    let incl: [&str; 0] = [];

    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("src/g_rpc/generated")
        .compile_protos(
            &[
                "src/g_rpc/protos/proto/controls/service/DAQ/v1/DAQ.proto",
                "src/g_rpc/protos/proto/controls/service/ACLK/v1/ACLK.proto",
                "src/g_rpc/protos/proto/controls/common/v1/status.proto",
                "src/g_rpc/protos/proto/controls/third-party/interval.proto",
                "src/g_rpc/protos/proto/controls/common/v1/drf.proto",
                "src/g_rpc/protos/proto/controls/common/v1/event.proto",
                "src/g_rpc/protos/proto/controls/common/v1/sources.proto",
            ],
            &["src/g_rpc/protos"],
        )?;

    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_protos(&["src/g_rpc/xform/XForm.proto"], &incl)?;

    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_protos(&["src/g_rpc/wscan/WScan.proto"], &incl)?;

    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["src/g_rpc/devdb/DevDB.proto"], &incl)?;

    tonic_build::configure()
        .build_client(true)
        .build_server(false)
	.out_dir("src/g_rpc/generated")
        .compile_protos(
            &["proto/controls/service/TlgPlacement/v1/TlgPlacement.proto"],
            &["src/g_rpc/protos"],
        )?;

    Ok(())
}
