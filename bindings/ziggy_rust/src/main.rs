use std::io::{self, Read, Write};
use std::net::TcpStream;

const SERVER_IP: &str = "127.0.0.1";
const SERVER_PORT: u16 = 8080;
const BUFFER_SIZE: usize = 4096;
const TIMEOUT_SECS: u64 = 10;

fn send_and_receive(stream: &mut TcpStream, command: &str) -> io::Result<()> {
    let mut command_with_newline = String::from(command);
    if !command_with_newline.ends_with('\n') {
        command_with_newline.push('\n');
    }

    println!("\n>>> {}", command_with_newline.trim());

    stream.write(command_with_newline.as_bytes());
    std::thread::sleep(std::time::Duration::from_millis(50));
    stream.flush()?;

    let mut buffer = vec![0; BUFFER_SIZE];
    match stream.read(&mut buffer) {
        Ok(bytes_read) => {
            if bytes_read > 0 {
                let response = String::from_utf8_lossy(&buffer[..bytes_read]);
                println!("<<< {}", response.trim());
            } else {
                println!("(no response)");
            }
        }
        Err(e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {
            println!("timeout waiting for response");
        }
        Err(e) => return Err(e),
    }

    Ok(())
}

fn connect_to_ziggy(ip: &str, port: u16) -> io::Result<()> {
    println!("Connecting to {}:{}...", ip, port);

    let mut stream = match TcpStream::connect(format!("{}:{}", ip, port)) {
        Ok(s) => s,
        Err(ref e) if e.kind() == io::ErrorKind::ConnectionRefused => {
            eprintln!("Connection refused. is it running {}:{}?", ip, port);
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                e.to_string(),
            ));
        }
        Err(e) => return Err(e),
    };

    send_and_receive(&mut stream, "SET foo bar")?;
    send_and_receive(&mut stream, "GET foo")?;
    send_and_receive(&mut stream, "SET x 42")?;

    send_and_receive(&mut stream, "EXIT")?;

    println!("Testing done.\n");

    drop(stream);

    Ok(())
}

fn main() {
    if let Err(e) = connect_to_ziggy(SERVER_IP, SERVER_PORT) {
        eprintln!("Client error: {}", e);
        std::process::exit(1);
    }
}
