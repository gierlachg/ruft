use std::error::Error;

use bytes::Bytes;
use log::LevelFilter;
use log4rs::{
    append::console::ConsoleAppender,
    config::{Appender, Config, Root},
};
use tokio;

use ruft_client::RuftClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logger();

    tokio::spawn(async {
        let mut client = RuftClient::new(vec!["127.0.0.1:8080".parse().unwrap()]).await.unwrap();

        client.store(Bytes::from_static(&[1])).await;
    })
    .await?;

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
