use kafka::run_kafka;

use clap::Parser;
use std::net::IpAddr;

#[derive(clap::Parser, Debug)]
#[command(
    author,
    version,
    about = r#"Kafka is a service that provides an interface for exchanging messages between different applications on different devices.
Users can subscribe to topics using sending {"method": "subscribe", "topic": "topic_name"}
And start sending to topics using {"method": "publish", "topic": "topic_name"}
After the publish message there are messages that the publisher sends to the topic:
{"message": "Hello world!"}"#
)]
struct Args {
    /// kafka server address
    #[arg(short, long)]
    address: IpAddr,

    /// kafka server port
    #[arg(short, long)]
    port: u16,
}

/// main function is used just to launch lib kafka function
fn main() {
    let args = Args::parse();
    run_kafka(args.address, args.port);
}
