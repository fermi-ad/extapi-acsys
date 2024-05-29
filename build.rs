fn main() -> Result<(), Box<dyn std::error::Error>> {
    let incl: [&str; 0] = [];

    println!("cargo:rerun-if-changed=src/g_rpc/dpm/deviceinfo.proto");

    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .compile(&["src/g_rpc/dpm/dpm.proto"], &incl)?;

    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .compile(&["src/g_rpc/clock/clock_event.proto"], &incl)?;

    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .compile(&["src/g_rpc/xform/XForm.proto"], &incl)?;

    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["src/g_rpc/devdb/DevDB.proto"], &incl)?;

    Ok(())
}
