use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;
use crate::storage::manager::StorageManager;
use crate::access::transaction::manager::TransactionManager;

pub struct DatabaseServer {
    pub storage: Arc<StorageManager>,
    pub tm: Arc<TransactionManager>,
}

impl DatabaseServer {
    pub async fn run(&self, addr: &str) -> tokio::io::Result<()> {
        let listener: TcpListener = TcpListener::bind(addr).await?;
        println!("[RG-Server] Poslušam na {}", addr);

        loop {
            // Vsaka nova povezava dobi svojo nit (Tokio task)
            let (socket, _) = listener.accept().await?;
            let storage = self.storage.clone();
            let tm = self.tm.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, storage, tm).await {
                    println!("Napaka pri povezavi: {}", e);
                }
            });
        }
    }
}

pub async fn handle_connection(
    mut socket: TcpStream, 
    storage: Arc<StorageManager>,
    tm: Arc<TransactionManager>
) -> tokio::io::Result<()> {
    let mut buffer = [0u8; 1024];

    loop {
        let n = socket.read(&mut buffer).await?;
        if n == 0 { return Ok(()); } // Povezava zaprta

        // Enostavna interpretacija ukazov (zaenkrat tekstovnih)
        let request = String::from_utf8_lossy(&buffer[..n]);
        let response = if request.trim() == "FLUSH" {
            storage.get_bpm().flush_all();
            "SUCCESS: Flushed all pages.\n"
        } else if request.starts_with("SELECT") {
            // Tukaj bi poklical tvoj HeapScan in vrnil podatke
            "DATA: [Tukaj bodo rezultati tvojega scana]\n"
        } else {
            "ERROR: Unknown command.\n"
        };

        socket.write_all(response.as_bytes()).await?;
    }
}