use std::sync::Arc;
use std::thread;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

// Uvoz tvojih modulov
use rustgress::storage::buffer::manager::BufferPoolManager;
use rustgress::storage::manager::StorageManager;
use rustgress::access::transaction::manager::TransactionManager;
use rustgress::catalog::manager::CatalogManager;
use rustgress::query::parser::parser::*;
use rustgress::query::executor::executor::ExecutionEngine; 
use rustgress::query::json::translator::WebTranslator;

fn main() {
    println!("=== RUSTGRESS HTTP SERVER ŠTARTUJE ===");

    // 1. Nastavitev deljenih sistemskih komponent
    let bpm = Arc::new(BufferPoolManager::new(50));
    let sm = Arc::new(StorageManager::new(bpm.clone()));
    let tm = Arc::new(TransactionManager::new());
    let cm = Arc::new(CatalogManager::new(sm.clone(), tm.clone()));
    
    // Naložimo sistemske kataloge ali jih inicializiramo
    let completed = cm.bootstrap_system_catalogs();
    if completed { // system catalogs were created for the first time
        let engine = ExecutionEngine::new(bpm.clone(), sm.clone(), tm.clone(), cm.clone());
        run_bootstrap_scripts(&engine, "src/utils/rgsql_scripts/bootstrap");
    };
    println!("[Server] Sistemski katalogi so pripravljeni.");

    // 2. Zaženemo TCP Listener na vratih 8080
    let listener = TcpListener::bind("127.0.0.1:8080").expect("Ni mogoče zasedati vrat 8080");
    println!("[Server] Poslušam na http://127.0.0.1:8080 ...");

    // Neskončna zanka, ki sprejema odjemalce
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let bpm_c = bpm.clone();
                let sm_c = sm.clone();
                let tm_c = tm.clone();
                let cm_c = cm.clone();

                // Za vsako povezavo ustvarimo novo nit
                thread::spawn(move || {
                    handle_connection(stream, bpm_c, sm_c, tm_c, cm_c);
                });
            }
            Err(e) => {
                eprintln!("[Server] Napaka pri sprejemanju povezave: {}", e);
            }
        }
    }
}

/// Skrbi za posamezno HTTP povezavo
fn handle_connection(
    mut stream: TcpStream,
    bpm: Arc<BufferPoolManager>,
    sm: Arc<StorageManager>,
    tm: Arc<TransactionManager>,
    cm: Arc<CatalogManager>,
) {
    let mut buffer = [0; 2048];
    if let Err(e) = stream.read(&mut buffer) {
        eprintln!("Napaka pri branju iz streama: {}", e);
        return;
    }

    let request = String::from_utf8_lossy(&buffer[..]);
    
    // Podpiramo samo POST zahteve
    if request.starts_with("POST") {
        if let Some(body_start) = request.find("\r\n\r\n") {
            let sql_query = request[body_start + 4..].trim().trim_matches('"');
            
            if sql_query.is_empty() {
                send_http_error(&mut stream, "Prazen SQL ukaz.");
                return;
            }

            println!("[Server] Prejet SQL: \"{}\"", sql_query);

            // 3. PARSANJE IN IZVEDBA POIZVEDBE
            let mut parser = SQLParser::new(sql_query);
            match parser.parse_statement() {
                Ok(statement) => {
                    let engine = ExecutionEngine::new(bpm.clone(), sm.clone(), tm.clone(), cm.clone());
                    
                    // Zaženemo transakcijo
                    let xid = tm.begin();
                    
                    // --- POPRAVEK: Lovimo tako rezultate kot dinamično shemo iz executorja ---
                    let (rezultati, izhodna_shema) = 
                        match engine.execute_statement(statement, xid) {
                            Ok((res, shema)) => (res, shema),
                            Err(e) => {
                                tm.abort(xid);
                                send_http_error(&mut stream, &format!("Execution Error: {}", e));
                                return;
                            }
                        };
                    
                    tm.commit(xid);
                    bpm.flush_all(); 

                    // --- POPRAVEK: Nič več trdo kodiranega iskanja sheme iz CatalogManagerja! ---
                    // Translatorju pošljemo natančno tisto shemo, ki jo je sestavil ProjectionExecutor.
                    let json_output = WebTranslator::to_web_json(&izhodna_shema, &rezultati);

                    // 5. POŠILJANJE HTTP ODGOVORA S CORS PODPORO
                    let response = format!(
                        "HTTP/1.1 200 OK\r\n\
                        Content-Type: application/json\r\n\
                        Access-Control-Allow-Origin: *\r\n\
                        Access-Control-Allow-Methods: POST, GET, OPTIONS\r\n\
                        Content-Length: {}\r\n\r\n\
                        {}",
                        json_output.len(),
                        json_output
                    );

                    let _ = stream.write_all(response.as_bytes());
                }
                Err(napaka) => {
                    send_http_error(&mut stream, &format!("SQL Parser Error: {}", napaka));
                }
            }
        }
    } else if request.starts_with("OPTIONS") {
        let response = "HTTP/1.1 204 No Content\r\n\
                        Access-Control-Allow-Origin: *\r\n\
                        Access-Control-Allow-Methods: POST, GET, OPTIONS\r\n\
                        Access-Control-Allow-Headers: Content-Type\r\n\r\n";
        let _ = stream.write_all(response.as_bytes());
    } else {
        send_http_error(&mut stream, "Podpiramo samo POST zahteve z SQL telesom.");
    }
    
    let _ = stream.flush();
}

fn send_http_error(stream: &mut TcpStream, error_msg: &str) {
    let error_json = format!(
        "{{\"status\":\"error\",\"message\":\"{}\"}}",
        error_msg.replace('"', "\\\"")
    );
    let response = format!(
        "HTTP/1.1 400 Bad Request\r\n\
        Content-Type: application/json\r\n\
        Access-Control-Allow-Origin: *\r\n\
        Content-Length: {}\r\n\r\n\
        {}",
        error_json.len(),
        error_json
    );
    let _ = stream.write_all(response.as_bytes());
}

/// Additional scripts for inicialization.
fn run_bootstrap_scripts(engine: &ExecutionEngine, folder_path: &str) {
    use std::fs;
    let path = std::path::Path::new(folder_path);
    if !path.is_dir() { return; }
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("rgsql") {
                let code = fs::read_to_string(&path).unwrap();
                let mut parser = SQLParser::new(&code);
                match parser.parse_script() {
                    Ok(statements) => {
                        for statement in statements {
                            let xid = engine.tm.begin();
                            match engine.execute_statement(statement, xid) {
                                Ok(_) => {
                                    // 3. Commit
                                    engine.tm.commit(xid);
                                },
                                Err(e) => {
                                    // 4. Abort ob napaki
                                    engine.tm.abort(xid);
                                    eprintln!("[Bootstrap] Napaka pri ukazu v {}: {}", path.display(), e);
                                    break;
                                }
                            }
                        }
                        println!("[Bootstrap] USPEH ({})", path.display());
                    }
                    Err(e) => eprintln!("[Bootstrap] Parser error v {}: {}", path.display(), e),
                }
            }
        }
    }
}



// --- THIS VERSION BELOW IS better, but currently heap scan doesn't know its own transaction id, and 
//     it can not see changes made in its transaction. We shouldn't just add xid to heap scan as it is wasteful,
//     perhaps we should somehow get it to snapshot.

// fn run_bootstrap_scripts(engine: &ExecutionEngine, folder_path: &str) {
//     use std::fs;
//     let path = std::path::Path::new(folder_path);
//     if !path.is_dir() { return; }

//     if let Ok(entries) = fs::read_dir(path) {
//         for entry in entries.flatten() {
//             let path = entry.path();
//             if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("rgsql") {
//                 let code = fs::read_to_string(&path).unwrap();
                
//                 // --- TU JE SPREMEMBA: Vsaka datoteka je ena transakcija ---
//                 let xid = engine.tm.begin(); 
                
//                 match engine.run_script_in_transaction(&code, xid) {
//                     Ok(_) => {
//                         engine.tm.commit(xid);
//                         println!("[Bootstrap] USPEH ({})", path.display());
//                     },
//                     Err(err) => {
//                         engine.tm.abort(xid);
//                         eprintln!("[Bootstrap] NAPAKA ({}): {}", path.display(), err);
//                     }
//                 }
//             }
//         }
//     }
// }