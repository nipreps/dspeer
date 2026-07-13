mod node;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    node::peer().await?;
    Ok(())
}