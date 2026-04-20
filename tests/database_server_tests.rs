// #[cfg(test)]
// mod tests {
//     use super::*;
//     use rustgress::storage::buffer::manager::BufferPoolManager;
//     use tokio::io::{AsyncReadExt, AsyncWriteExt};
//     use std::time::Duration;
//     use rustgress::server::database_server::{DatabaseServer, handle_connection};
//     use rustgress::storage::manager::StorageManager;
//     use rustgress::access::transaction::manager::TransactionManager;
//     use tokio::net::TcpListener;
//     use std::sync::Arc;
//     use tokio::net::TcpStream;
    

//     #[tokio::test]
//     async fn test_server_run_method() {
//         // 1. Priprava okolja
//         let bpm = Arc::new(BufferPoolManager::new(50));
//         let storage = Arc::new(StorageManager::new(bpm.clone())); 
//         let tm = Arc::new(TransactionManager::new());
        
//         let server = Arc::new(DatabaseServer {
//             storage: storage.clone(),
//             tm: tm.clone(),
//         });

//         // 2. Izberemo naslov (port 0 pusti OS-u, da izbere prost port)
//         let addr = "127.0.0.1:0";
        
//         // Ker se run() ne konča, moramo ugotoviti, na katerem portu je pristal, 
//         // še preden ga poženemo, ali pa uporabiti fiksnega (npr. 127.0.0.1:8888).
//         // Za ta test uporaba fiksnega porta 8888 poenostavi stvari:
//         let test_addr = "127.0.0.1:8888";

//         // 3. DEJANSKO TESTIRAMO server.run()
//         let server_ptr = server.clone();
//         tokio::spawn(async move {
//             // Ta klic se zdaj izvaja v ozadju
//             if let Err(e) = server_ptr.run(test_addr).await {
//                 eprintln!("Server error: {}", e);
//             }
//         });

//         // Počakamo sekundo, da se server v ozadju zares zažene
//         tokio::time::sleep(Duration::from_millis(100)).await;

//         // 4. Client se poveže na tvoj "run" server
//         let mut stream = TcpStream::connect(test_addr).await
//             .expect("Server se ni odzval na portu 8888");

//         // 5. Testiramo komunikacijo
//         stream.write_all(b"FLUSH").await.unwrap();

//         let mut buffer = [0u8; 1024];
//         let n = stream.read(&mut buffer).await.unwrap();
//         let response = String::from_utf8_lossy(&buffer[..n]);

//         assert!(response.contains("SUCCESS: Flushed all pages."));
//         println!("Test uspešen: Server run() se odziva!");
//     }
// }