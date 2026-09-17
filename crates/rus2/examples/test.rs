use rus2::beacon::hello_world;
use rus2::beacon;
use rus2::node;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("hello");
    hello_world().await?;

    let node = node::Node::builder("test").spawn().await?;

    // let b = beacon::Beacon::new(node);

    // b.run().await?;

    tokio::signal::ctrl_c().await?;
    

    Ok(())
}