fn main() -> Result<(), Box<dyn std::error::Error>> {
    let incl: [&str; 0] = [];

    println!("cargo:rerun-if-changed=src/g_rpc/dpm/deviceinfo.proto");

    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("src/g_rpc/generated")
        .compile_protos(
            &[
                "src/g_rpc/protos/services/DAQ.proto",
                "src/g_rpc/protos/services/ACLK.proto",
                "src/g_rpc/protos/common/status.proto",
                "src/g_rpc/protos/third-party/interval.proto",
                "src/g_rpc/protos/common/drf.proto",
                "src/g_rpc/protos/common/event.proto",
                "src/g_rpc/protos/common/sources.proto",
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

    Ok(())
}
