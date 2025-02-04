mod broadcast;
mod crdt;
mod datomic;
mod maelstrom;
mod maelstrom_generic;

#[tokio::main]
async fn main() {
    crdt::g_counter::run().await
}
