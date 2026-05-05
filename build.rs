fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    tonic_prost_build::configure()
        .build_server(true)
        .compile_protos(
            &["proto/services.proto"], // Path to your proto file
            &["proto"], // Directory where the proto file is located
        )?;
    Ok(())
}