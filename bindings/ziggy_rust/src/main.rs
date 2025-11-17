use crate::client::Client;

mod database;
mod ffi;
mod client;
use std::{io, thread::sleep, time::Duration};


fn main() -> io::Result<()> {
    let mut client = Client::connect("127.0.0.1:8080")?;
    println!("Connected!");
    
    if let Some(greeting) = client.recv_greeting()? {
        println!("Server: {}", greeting);
    }
    
    for msg in client.listen()? {
        match msg {
            Ok(line) => println!("Server: {}", line),
            Err(_) => {
                println!("Connection closed");
                break;
            }
        }
        sleep(Duration::from_millis(100000000000));
    }
    
    
    Ok(())
}
