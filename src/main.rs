mod broadcast;
mod crdt;
mod datomic;
mod maelstrom;
mod maelstrom_generic;

#[tokio::main]
async fn main() {
    datomic::transactor::run().await;
}
