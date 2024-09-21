use anyhow::Result;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};
use tracing::{info, warn};

const BUF_SIZE: usize = 4096;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let addr = "0.0.0.0:6379";
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on {}", addr);
    loop {
        let (stream, raddr) = listener.accept().await?;
        info!("Accepted connection from {}", raddr);
        tokio::spawn(async move {
            if let Err(e) = process_redis_conn(stream).await {
                warn!("Error processing connection: {:?}", e);
            }
        });
    }
}

async fn process_redis_conn(mut stream: TcpStream) -> Result<()> {
    loop {
        stream.readable().await?;
        let mut buf = vec![0; BUF_SIZE];

        match stream.try_read_buf(&mut buf) {
            Ok(0) => {
                break;
            }
            Ok(n) => {
                info!("Received {} bytes", n);
                let line = String::from_utf8_lossy(&buf[..n]);
                info!("Received: {}", line);
                stream.write_all(b"+Ok\r\n").await?;
            }

            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }

    Ok(())
}
