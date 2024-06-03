use crate::connection_handlers::publisher_connection::handle_publisher;
use serde::{Deserialize, Serialize};
use serde_json;
use std::net::Shutdown;
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    net::TcpStream,
    sync::{Arc, Mutex},
};
/// handles users connection, waits for the first subscribe/publish message and launches handle_publisher if needed
/// * `user_stream` - `TcpStream` tcp stream of connected client
/// * `topics` - `Arc<Mutex<HashMap<String, Vec<TcpStream>>>>` map with topic:list of subscribers streams relation
pub(crate) fn handle_init_connection(
    mut user_stream: TcpStream,
    topics: Arc<Mutex<HashMap<String, Arc<Mutex<Vec<TcpStream>>>>>>,
) {
    let reader_binding = user_stream.try_clone().unwrap();
    let mut reader = BufReader::new(&reader_binding);

    let mut buf = String::new();

    #[derive(Serialize, Deserialize, Debug)]
    struct ConnectionRequestJson {
        method: String,
        topic: String,
    }

    match reader.read_line(&mut buf) {
        Ok(0) => {
            let _ = user_stream.shutdown(Shutdown::Both);
            return;
        }
        Ok(_) => match serde_json::from_str::<ConnectionRequestJson>(&buf) {
            Ok(request_json) => {
                println!("Got JSON: {:#?}", request_json);
                if request_json.method == "subscribe" {
                    let mut guard = topics.lock().unwrap();
                    if let Some(sub_streams) = guard.get(&request_json.topic) {
                        let arc_streams = Arc::clone(&sub_streams);
                        let mut sub_guard = arc_streams.lock().unwrap();
                        sub_guard.push(user_stream.try_clone().expect("error at clone"));
                    } else {
                        guard.insert(
                            request_json.topic.to_string(),
                            Arc::new(Mutex::new(vec![user_stream
                                .try_clone()
                                .expect("error at clone")])),
                        );
                    }
                    println!(
                        r#"For topic "{}" connected subscriber with ip {}"#,
                        request_json.topic,
                        user_stream.peer_addr().unwrap().ip()
                    );
                } else if request_json.method == "publish" {
                    println!(
                        r#"For topic "{}" connected publisher with ip {}"#,
                        request_json.topic,
                        user_stream.peer_addr().unwrap().ip()
                    );
                    handle_publisher(&request_json.topic, reader, user_stream, topics);
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
                    .write_all(&['\n' as u8])
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
