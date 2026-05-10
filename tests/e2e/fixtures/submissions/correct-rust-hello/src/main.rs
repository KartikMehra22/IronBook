use std::io::{Read, Write};
use std::net::TcpListener;

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:7777")?;
    eprintln!("hello-engine listening on :7777");
    for stream in listener.incoming() {
        if let Ok(mut s) = stream {
            let mut buf = [0u8; 1024];
            let _ = s.read(&mut buf);
            let _ = s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nack\n");
        }
    }
    Ok(())
}
// retry 1778397873
