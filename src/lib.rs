use std::{
    collections::HashMap,
    net::{IpAddr, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
};

pub mod connection_handlers;
use crate::connection_handlers::init_connection::handle_init_connection;

/// launches tcp server, connections are being accepted here
/// # Arguments
/// * `ip` - `IpAddr` ip address where the kafka server will be launched
/// * `port` - `u16` port of kafka server
/// arguments are directly passed from main function
pub fn run_kafka(ip: IpAddr, port: u16) {
    match TcpListener::bind(format!("{}:{}", ip, port)) {
        Ok(listener) => {
            println!("Start kafka server on address {}:{}", ip, port);

            let topics: Arc<Mutex<HashMap<String, Arc<Mutex<Vec<TcpStream>>>>>> =
                Arc::new(Mutex::new(HashMap::new()));

            for stream in listener.incoming() {
                let user_stream = stream.unwrap();
                println!("New connection from {}", user_stream.peer_addr().unwrap());
                let cloned_topics = Arc::clone(&topics);
                thread::spawn(move || handle_init_connection(user_stream, cloned_topics));
            }
        }
        Err(_) => {
            eprintln!(
                "Not able to start server on {}:{} -- port is busy",
                ip, port
            );
            std::process::exit(1);
        }
    }
}
