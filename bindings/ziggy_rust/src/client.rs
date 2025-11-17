use std::{net::{TcpStream, ToSocketAddrs}, sync::mpsc::Iter, time::Duration};
use std::io::{self, Read, Write};

const BUFFER_SiZE: usize = 1024;

pub struct Client {
    stream: TcpStream,
    buffer: [u8; BUFFER_SiZE],
}

impl Client {
    /// Connect to ZiggyDB server at given address (e.g: "127.0.0.1:8080")
    pub fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        
        Ok(Self {
            stream,
            buffer: [0; BUFFER_SiZE],
        })
    }
    
    pub fn recv_greeting(&mut self) -> io::Result<Option<String>> {
        match self.stream.read(&mut self.buffer) {
            Ok(0) => Ok(None),
            Ok(n) => Ok(Some(Self::decode(&self.buffer[..n]))),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
    }
    
    /// recieve a single response
    pub fn recv_response (&mut self) -> io::Result<Option<String>> {
        match self.stream.read(&mut self.buffer) {
            Ok(0) => Ok(None),
            Ok(n) => Ok(Some(Self::decode(&self.buffer[..n]))),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
    }
    
    pub fn listen(&mut self) -> io::Result<Listen<'_>> {
        Ok(Listen {client: self })
    }
    
    fn decode(bytes: &[u8]) -> String {
        String::from_utf8_lossy(bytes).trim_end().to_string()
    }
}

pub struct Listen<'a> {
    client: &'a mut Client,
}

impl <'a> Iterator for Listen<'a> {
    type Item = io::Result<String>;
    
    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = [0u8; BUFFER_SiZE];
        match self.client.stream.read(&mut buf) {
            Ok(0) => None,
            Ok(n) => Some(Ok(Client::decode(&buf[..n]))),
            Err(e) => Some(Err(e)),
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
    }
}