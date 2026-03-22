/// Regenerates `src/rti.rs` from the `.proto` files in `src/raw-proto/`.
/// Run via: `cargo run --example generate_protos && cargo fmt`
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").map_err(
        |_| "CARGO_MANIFEST_DIR not set — run this via `cargo run --example generate_protos`",
    )?);
    let src_dir = manifest_dir.join("src");
    let proto_dir = src_dir.join("raw-proto");

    let mut config = prost_build::Config::new();
    config.out_dir(&src_dir);
    config.compile_protos(&[proto_dir.join("otps_proto_pool.proto")], &[&proto_dir])?;

    // MessageType is in a separate proto not referenced by the pool,
    // so we compile it separately and append it to the generated file.
    config.out_dir(std::env::temp_dir());
    config.compile_protos(&[proto_dir.join("message_type.proto")], &[&proto_dir])?;

    let message_type_code = std::fs::read_to_string(std::env::temp_dir().join("rti.rs"))?;

    // Read the pool-generated file, prepend `pub mod messages;` and append MessageType
    let pool_path = src_dir.join("rti.rs");
    let pool_code = std::fs::read_to_string(&pool_path)?;

    let combined = format!("pub mod messages;\n\n{message_type_code}\n{pool_code}");
    std::fs::write(&pool_path, combined)?;

    println!("Generated src/rti.rs — run `cargo fmt` to format it.");
    Ok(())
}
