use rus2::beacon::hello_world;
use rus2::beacon;
use rus2::node;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug)]
struct TestStruct {
    test_int: u64
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("hello");
    hello_world().await?;

    let node = node::Node::builder("TEST_NODE").spawn().await?;

    // let b = beacon::Beacon::new(node);
    let test = TestStruct{test_int: 5};

    // b.run().await?;

    tokio::signal::ctrl_c().await?;
    node.publish::<TestStruct>("TEST TOPIC", &test).await?;
    

    Ok(())
}