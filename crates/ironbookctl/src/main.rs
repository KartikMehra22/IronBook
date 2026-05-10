// `anyhow::Result<()>` will return once Phase 4 introduces fallible commands.
#[allow(clippy::unnecessary_wraps)]
fn main() -> anyhow::Result<()> {
    println!("ironbookctl v{}", env!("CARGO_PKG_VERSION"));
    Ok(())
}
