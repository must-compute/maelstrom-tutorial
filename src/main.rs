mod broadcast;
mod crdt;
mod maelstrom;
mod maelstrom_generic;

#[tokio::main]
async fn main() {
    crdt::g_set::run().await
}
