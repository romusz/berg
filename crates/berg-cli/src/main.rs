use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "berg", version, about = "Command-line interface for Berg.")]
struct Args;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _args = Args::parse();

    println!("{}", berg_core::welcome_message("berg")?);

    Ok(())
}
