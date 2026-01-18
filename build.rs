use std::io::Result;
fn main() -> Result<()> {
	prost_build::compile_protos(&["src/server/stream_extractor/np2_structs.proto"], &["src/server/stream_extractor/"])?;
	Ok(())
}
