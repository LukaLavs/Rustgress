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
use rustgress::utils::debug::errors::{RustgressError as RGE};
use std::sync::mpsc;
use rustgress::access::transaction::context::{get_thread_error};
use ctrlc;
use rustgress::access::heap::vaccum::Vacuum;

#[derive(Debug, PartialEq)]
enum ServerSignal {
    Restart,
    Shutdown,
}

fn main() {
    println!("Rustgress Server has Started!");

    let bpm = Arc::new(BufferPoolManager::new(50));
    let sm = Arc::new(StorageManager::new(bpm.clone()));
    let tm = Arc::new(TransactionManager::new());
    let cm = Arc::new(CatalogManager::new(sm.clone(), tm.clone()).unwrap()); // TODO: handle error
    let bpm_exit = bpm.clone();
    let tm_exit = tm.clone();
    let sm_exit = sm.clone();
    // On first inicalization we create system catalogs and run startup .rgsql scripts.
    let db_inicialization = cm.bootstrap_system_catalogs().unwrap();
    if db_inicialization {
        let engine = ExecutionEngine::new(bpm.clone(), sm.clone(), tm.clone(), cm.clone());
        match run_bootstrap_scripts(&engine, "src/utils/rgsql_scripts/bootstrap") {
            Ok(()) => println!("Bootstrap scripts executed successfully."),
            Err(e) => eprintln!("Error during bootstrap script execution: {}", e),
        }
    };
    println!("System catalogs initialized.");

    let (tx, rx) = mpsc::channel::<ServerSignal>(); // channel for signaling server shutdown
    let tx_loop = tx.clone();
    let tx_ctrlc = tx.clone();

    ctrlc::set_handler(move || {
        println!("Shutdown signal received, shutting down server...");
        let _ = tx_ctrlc.send(ServerSignal::Shutdown);
    }).expect("Error setting Ctrl-C handler");

    let listener = TcpListener::bind("127.0.0.1:8080").expect("Perhaps port 8080 is already in use?");
    println!("Server listening on http://127.0.0.1:8080");

    thread::spawn(move || {
        // handle incoming connections in a loop
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let bpm_c = bpm.clone();
                    let sm_c = sm.clone();
                    let tm_c = tm.clone();
                    let cm_c = cm.clone();
                    let tx_conn = tx_loop.clone();

                    // spawn new thread for each connection
                    thread::spawn(move || {
                        handle_connection(stream, bpm_c, sm_c, tm_c, cm_c, tx_conn);
                    });
                }
                Err(e) => {
                    eprintln!("Server error when receiving connection: {}", e);
                }
            }
        }
    });

    // Main thread exit
    let signal = rx.recv().unwrap_or(ServerSignal::Shutdown); // wait for shutdown signal from connection handler
    if signal == ServerSignal::Shutdown {
        println!("Performing system maintenance before shutdown...");
        let vacuum = Vacuum::new(bpm_exit.clone(), tm_exit.clone(), sm_exit.clone());
        match vacuum.vacuum_all_tables() {
            Ok(()) => println!("Cluster-wide vacuum completed successfully."),
            Err(e) => eprintln!("Cluster-wide vacuum failed: {:?}", e),
        };
        match bpm_exit.clone().flush_all() {
            Ok(()) => println!("Buffer pool flushed successfully."),
            Err(e) => eprintln!("Failed to flush buffer pool: {:?}", e),
        }
        std::process::exit(0);
    } else { // ServerSignal::Restart
        bpm_exit.clone().flush_all().expect("Failed to flush buffer pool before restart");
        println!("Server is restarting due to critical error.");
        restart_server_process();
    }
}

/// Handle a single client connection: read the request, execute the SQL, and send back the response.
fn handle_connection(
    mut stream: TcpStream,
    bpm: Arc<BufferPoolManager>,
    sm: Arc<StorageManager>,
    tm: Arc<TransactionManager>,
    cm: Arc<CatalogManager>,
    tx: mpsc::Sender<ServerSignal>,
) -> Option<()> {
    let mut buffer = [0; 2048];
    if let Err(e) = stream.read(&mut buffer) {
        eprintln!("Server error when reading from stream: {}", e);
        return Some(());
    }
    let request = String::from_utf8_lossy(&buffer[..]);
    
    // Accept POST requests with SQL in the body.
    if request.starts_with("POST") {
        if let Some(body_start) = request.find("\r\n\r\n") {
            let sql_query = request[body_start + 4..].trim().trim_matches('"');
            
            if sql_query.is_empty() {
                send_http_error(&mut stream, "Server received empty SQL query.");
                return Some(());
            }

            println!("Server recieved SQL query: \"{}\"", sql_query);

            // Parsing and executing the SQL query
            let mut parser = SQLParser::new(sql_query);
            match parser.parse_statement() {
                Ok(statement) => {
                    let engine = ExecutionEngine::new(bpm.clone(), sm.clone(), tm.clone(), cm.clone());

                    let xid = tm.begin().unwrap(); // begin a transaction TODO: handle error
                    transaction::context::set_current_xid(xid); // save xid in thread-local context

                    let (rezultati, izhodna_shema) = 
                        match engine.execute_statement(statement) {
                            Ok((res, shema)) => (res, shema),
                            Err(e) => {
                                tm.abort(xid).unwrap();
                                send_http_error(&mut stream, &format!("Execution Error: {}", e));
                                return Some(());
                            }
                        };
                        if let Some(e) = get_thread_error() {
                            let aborted = tm.abort(xid).is_ok();
                            send_http_error(&mut stream, &format!("Critical Storage Error: {}. Transaction aborted: {}", e, aborted));
                            trigger_server_restart(tx.clone()); // send signal to main thread to restart the server
                            return None; // terminates the current connection thread without sending a response.
                        };
                    tm.commit(xid).map_err(|_| trigger_server_restart(tx.clone())).unwrap();
                    transaction::context::clear_current_xid(); // clear xid from thread-local context

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
    Some(())
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

fn run_bootstrap_scripts(engine: &ExecutionEngine, folder_path: &str) -> Result<(), RGE> {
    use std::fs;
    let path = std::path::Path::new(folder_path);
    if !path.is_dir() { panic!(); }
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("rgsql") {
                println!("[Bootstrap] Running script: {}", path.display());
                let code = fs::read_to_string(&path).unwrap();
                let xid = engine.tm.begin()?; // each file gets its own transaction id.
                transaction::context::set_current_xid(xid);
                match engine.run_script_in_transaction(&code) {
                    Ok(_) => {
                        engine.tm.commit(xid)?;
                        println!("[Bootstrap] Success ({})", path.display());
                    },
                    Err(err) => {
                        engine.tm.abort(xid)?;
                        eprintln!("[Bootstrap] Error ({}): {}", path.display(), err);
                    }
                }
                transaction::context::clear_current_xid();
            }
        }
    }
    Ok(())
}

fn restart_server_process() -> ! {
    use std::process::Command;
    use std::os::unix::process::CommandExt; // Works on Linux / macOS

    println!("Restarting process ...");

    let args: Vec<String> = std::env::args().collect();
    let current_exe = std::env::current_exe().expect("Failed to get current exe path");

    let error = Command::new(current_exe)
        .args(&args[1..])
        .exec();

    panic!("CRITICAL: Failed to execute server restart: {}", error);
}
fn trigger_server_restart(tx: mpsc::Sender<ServerSignal>) {
    let _ = tx.send(ServerSignal::Restart);
}