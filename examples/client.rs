use std::error::Error;

use log::{error, info, LevelFilter};
use log4rs::{
    append::console::ConsoleAppender,
    config::{Appender, Config, Root},
};
use tokio;

use ruft_client::RuftClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logger();

    let mut client = RuftClient::new(vec!["127.0.0.1:8080".parse().unwrap()], 5_000)
        .await
        .unwrap();
    match client.write("map", &1u64.to_le_bytes(), "1".as_bytes()).await {
        Ok(_) => {
            info!("Successfully stored");
        }
        Err(e) => {
            error!("Failed to store; error = {:?}", e);
        }
    }

    match client.read("map", &1u64.to_le_bytes()).await {
        Ok(response) => {
            info!(
                "Successfully read: {:?}",
                response.map(|value| String::from_utf8(value).unwrap())
            );
        }
        Err(e) => {
            error!("Failed to read; error = {:?}", e);
        }
    }

    Ok(())
}

fn init_logger() {
    let _ = log4rs::init_config(
        Config::builder()
            .appender(Appender::builder().build("stdout", Box::new(ConsoleAppender::builder().build())))
            .build(Root::builder().appender("stdout").build(LevelFilter::Info))
            .unwrap(),
    )
    .unwrap();
}
