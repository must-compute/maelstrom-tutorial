mod broadcast;
mod maelstrom;

#[tokio::main]
async fn main() {
    broadcast::r#async::run().await;
}
