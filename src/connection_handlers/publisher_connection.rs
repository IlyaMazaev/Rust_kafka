use serde::{Deserialize, Serialize};
use serde_json;
use std::net::Shutdown;
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    net::TcpStream,
    sync::{Arc, Mutex},
};

/// handles publishers connection, waits for the {"message": "Hello 1 from Rust publisher!"} and sends it to all topic subscribers
/// * `topic` - `&str` this publishers topic
/// * `reader` - `BufReader<&TcpStream>` this publishers tcp reader to listen for publishers messages
/// * `user_stream` - `TcpStream` tcp stream of connected client
/// * `topics` - `Arc<Mutex<HashMap<String, Vec<TcpStream>>>>` map with topic:list of subscribers streams relation
pub(crate) fn handle_publisher(
    topic: &str,
    mut reader: BufReader<&TcpStream>,
    mut user_stream: TcpStream,
    topics: Arc<Mutex<HashMap<String, Arc<Mutex<Vec<TcpStream>>>>>>,
) {
    loop {
        let mut buf = String::new();

        #[derive(Serialize, Deserialize, Debug)]
        struct MessageJson {
            message: String,
        }
        match reader.read_line(&mut buf) {
            Ok(0) => {
                let _ = user_stream.shutdown(Shutdown::Both);
                return;
            }
            Ok(_) => match serde_json::from_str::<MessageJson>(&buf) {
                Ok(message_json) => {
                    println!("Got JSON: {:#?}", message_json);

                    let topics_guard = topics.lock().unwrap();
                    if let Some(sub_streams) = topics_guard.get(topic) {
                        let streams_arc = Arc::clone(&sub_streams);
                        let mut streams_guard = streams_arc.lock().unwrap();
                        drop(topics_guard);
                        for subscriber_stream in streams_guard.iter_mut() {
                            let sent_res = serde_json::to_writer(
                                &*subscriber_stream,
                                &MessageJson {
                                    message: message_json.message.to_string(),
                                },
                            );
                            if sent_res.is_ok() {
                                subscriber_stream
                                    .write(&['\n' as u8])
                                    .expect("error at sending new line symbol");
                            }
                        }
                        streams_guard.retain(|x| x.peer_addr().is_ok());
                    } else {
                        println!("no subscribers or topic does not exist")
                    }
                }
                Err(err) => {
                    #[derive(Serialize, Deserialize)]
                    struct ParseJsonError {
                        error: String,
                    }

                    let err_json = "received not valid json";
                    serde_json::to_writer(
                        &user_stream,
                        &ParseJsonError {
                            error: err_json.to_string(),
                        },
                    )
                    .expect("error at sending json");
                    user_stream
                        .write(&['\n' as u8])
                        .expect("error at sending new line symbol");
                    println!(
                        r#"Failed to parse message from publisher {} with err: "{}", closing connection"#,
                        user_stream.peer_addr().unwrap().ip(),
                        err
                    );
                    user_stream
                        .shutdown(Shutdown::Both)
                        .expect("error on tcp shutdown");
                    return;
                }
            },
            Err(_) => {
                user_stream
                    .shutdown(Shutdown::Both)
                    .expect("error on tcp shutdown");
                return;
            }
        }
    }
}
