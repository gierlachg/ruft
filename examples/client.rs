use std::error::Error;

use tokio;
use tracing::{error, info, Level};

use ruft_client::RuftClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_tracing();

    let mut client = RuftClient::new(vec!["127.0.0.1:8080".parse()?], 5_000).await?;

    client.write("map", &1u64.to_le_bytes(), "1".as_bytes()).await?;
    info!("Successfully stored");

    let value = client.read("map", &1u64.to_le_bytes()).await?;
    match value.and_then(|value| String::from_utf8(value).ok()) {
        Some(value) if value == "1" => info!("Successfully read: {:?}", &value),
        _ => error!("That should not happen!"),
    }

    Ok(())
}

fn init_tracing() {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
}
