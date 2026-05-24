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
use rustgress::access::transaction;//context::{set_current_xid, clear_current_xid};

fn main() {
    println!("Rustgress Server has Started!");

    let bpm = Arc::new(BufferPoolManager::new(50));
    let sm = Arc::new(StorageManager::new(bpm.clone()));
    let tm = Arc::new(TransactionManager::new());
    let cm = Arc::new(CatalogManager::new(sm.clone(), tm.clone()));
    
    // On first inicalization we create system catalogs and run startup .rgsql scripts.
    let db_inicialization = cm.bootstrap_system_catalogs();
    if db_inicialization {
        let engine = ExecutionEngine::new(bpm.clone(), sm.clone(), tm.clone(), cm.clone());
        run_bootstrap_scripts(&engine, "src/utils/rgsql_scripts/bootstrap");
    };
    println!("System catalogs initialized.");

    let listener = TcpListener::bind("127.0.0.1:8080").expect("Perhaps port 8080 is already in use?");
    println!("Server listening on http://127.0.0.1:8080");

    // handle incoming connections in a loop
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let bpm_c = bpm.clone();
                let sm_c = sm.clone();
                let tm_c = tm.clone();
                let cm_c = cm.clone();

                // spawn new thread for each connection
                thread::spawn(move || {
                    handle_connection(stream, bpm_c, sm_c, tm_c, cm_c);
                });
            }
            Err(e) => {
                eprintln!("Server error when receiving connection: {}", e);
            }
        }
    }
}

/// Handle a single client connection: read the request, execute the SQL, and send back the response.
fn handle_connection(
    mut stream: TcpStream,
    bpm: Arc<BufferPoolManager>,
    sm: Arc<StorageManager>,
    tm: Arc<TransactionManager>,
    cm: Arc<CatalogManager>,
) {
    let mut buffer = [0; 2048];
    if let Err(e) = stream.read(&mut buffer) {
        eprintln!("Server error when reading from stream: {}", e);
        return;
    }
    let request = String::from_utf8_lossy(&buffer[..]);
    
    // Accept POST requests with SQL in the body.
    if request.starts_with("POST") {
        if let Some(body_start) = request.find("\r\n\r\n") {
            let sql_query = request[body_start + 4..].trim().trim_matches('"');
            
            if sql_query.is_empty() {
                send_http_error(&mut stream, "Server received empty SQL query.");
                return;
            }

            println!("Server recieved SQL query: \"{}\"", sql_query);

            // Parsing and executing the SQL query
            let mut parser = SQLParser::new(sql_query);
            match parser.parse_statement() {
                Ok(statement) => {
                    let engine = ExecutionEngine::new(bpm.clone(), sm.clone(), tm.clone(), cm.clone());

                    let xid = tm.begin(); // begin a transaction
                    transaction::context::set_current_xid(xid); // save xid in thread-local context

                    let (rezultati, izhodna_shema) = 
                        match engine.execute_statement(statement) {
                            Ok((res, shema)) => (res, shema),
                            Err(e) => {
                                tm.abort(xid);
                                send_http_error(&mut stream, &format!("Execution Error: {}", e));
                                return;
                            }
                        };
                    
                    tm.commit(xid);
                    transaction::context::clear_current_xid(); // clear xid from thread-local context
                    bpm.flush_all(); // tehnically we should only flush on program exit (this is here for testing)

                    let json_output = WebTranslator::to_web_json(&izhodna_shema, &rezultati);

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
        send_http_error(&mut stream, "Server supports only POST requests with SQL body.");
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
// fn run_bootstrap_scripts(engine: &ExecutionEngine, folder_path: &str) {
//     use std::fs;
//     let path = std::path::Path::new(folder_path);
//     if !path.is_dir() { return; }
//     if let Ok(entries) = fs::read_dir(path) {
//         for entry in entries.flatten() {
//             let path = entry.path();
//             if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("rgsql") {
//                 let code = fs::read_to_string(&path).unwrap();
//                 let mut parser = SQLParser::new(&code);
//                 match parser.parse_script() {
//                     Ok(statements) => {
//                         for statement in statements {
//                             let xid = engine.tm.begin();
//                             match engine.execute_statement(statement, xid) {
//                                 Ok(_) => {
//                                     // 3. Commit
//                                     engine.tm.commit(xid);
//                                 },
//                                 Err(e) => {
//                                     // 4. Abort ob napaki
//                                     engine.tm.abort(xid);
//                                     eprintln!("[Bootstrap] Napaka pri ukazu v {}: {}", path.display(), e);
//                                     break;
//                                 }
//                             }
//                         }
//                         println!("[Bootstrap] USPEH ({})", path.display());
//                     }
//                     Err(e) => eprintln!("[Bootstrap] Parser error v {}: {}", path.display(), e),
//                 }
//             }
//         }
//     }
// }



// --- THIS VERSION BELOW IS better, but currently heap scan doesn't know its own transaction id, and 
//     it can not see changes made in its transaction. We shouldn't just add xid to heap scan as it is wasteful,
//     perhaps we should somehow get it to snapshot.

fn run_bootstrap_scripts(engine: &ExecutionEngine, folder_path: &str) {
    use std::fs;
    let path = std::path::Path::new(folder_path);
    if !path.is_dir() { return; }
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("rgsql") {
                println!("[Bootstrap] Running script: {}", path.display());
                let code = fs::read_to_string(&path).unwrap();
                let xid = engine.tm.begin(); // each file gets its own transaction id.
                transaction::context::set_current_xid(xid);
                match engine.run_script_in_transaction(&code, xid) {
                    Ok(_) => {
                        engine.tm.commit(xid);
                        println!("[Bootstrap] Success ({})", path.display());
                    },
                    Err(err) => {
                        engine.tm.abort(xid);
                        eprintln!("[Bootstrap] Error ({}): {}", path.display(), err);
                    }
                }
                transaction::context::clear_current_xid();
            }
        }
    }
}