use std::sync::{Arc};
use std::thread;
use rustgress::storage::buffer::manager::BufferPoolManager;
use rustgress::storage::manager::StorageManager;
use rustgress::access::transaction::manager::TransactionManager;
use rustgress::catalog::manager::CatalogManager;
use rustgress::access::tuple::desc::{TupleDescriptor, Column};
use rustgress::catalog::types::{DataType, Value};
use rustgress::access::heap::access::HeapAccess;
use rustgress::access::heap::scan::HeapScan;
// use rustgress::query::parser::parser::*;
// use rustgress::query::executor::executor::ExecutionEngine; 
// use rustgress::query::json::translator::WebTranslator;


fn main() {
    // 1. Setup shared system components
    let bpm = Arc::new(BufferPoolManager::new(50));
    let sm = Arc::new(StorageManager::new(bpm.clone()));
    let tm = Arc::new(TransactionManager::new());
    let cm = Arc::new(CatalogManager::new(sm.clone(), tm.clone()));
    
    cm.bootstrap_system_catalogs();

    // 2. Define a schema
    let schema = Arc::new(TupleDescriptor::new(vec![
        Column { name: "user_id".to_string(), data_type: DataType::Integer },
        Column { name: "msg".to_string(), data_type: DataType::Varchar(100) },
    ]));

    // 3. Create the table
    let table_name = "messages";
    let xid_setup = tm.begin();
    let msg_table_oid = cm.create_table(xid_setup, table_name, 0, &schema);
    tm.commit(xid_setup);
   
    println!("[Main] Table '{}' created with OID: {}", table_name, msg_table_oid);

    // 4. Simulate 3 concurrent users
    let mut handles = vec![];
    for user_id in 1..=3 {
        let sm_c = sm.clone();
        let tm_c = tm.clone();
        let schema_c = schema.clone();
        
        let handle = thread::spawn(move || {
            let xid = tm_c.begin();
            for i in 1..=10 {
                let mut tuple = schema_c.pack(vec![
                    Value::Integer(user_id as i32),
                    Value::Varchar(format!("Hello from user {}, msg #{}", user_id, i)),
                ], 0);
                HeapAccess::insert(sm_c.clone(), xid, msg_table_oid, &mut tuple);
            }
            tm_c.commit(xid);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // 5. Verification: Scan data
    println!("\n--- Final Scan (Reading all committed data) ---");
    let table_handle = sm.get_table(msg_table_oid);
    let scan = HeapScan::new(bpm.clone(), table_handle, tm.clone());
    
    let mut count = 0;
    for tuple in scan {
        let _data = schema.unpack_from_tuple(&tuple);
        count += 1;
    }
    println!("Actual rows: {}", count);

    // ==========================================================
    // 6. NOVO: TESTIRANJE GET_SCHEMA (Dynamic Catalog Lookup)
    // ==========================================================
    println!("\n--- Catalog Verification ---");
    
    // Poskusimo dobiti shemo nazaj iz sistemskih katalogov samo z uporabo OID-ja
    let dynamic_schema = cm.get_schema(msg_table_oid);
    
    println!("Retrieved Schema for OID {}:", msg_table_oid);
    for (i, col) in dynamic_schema.columns.iter().enumerate() {
        println!("  Col #{}: {} ({:?})", i + 1, col.name, col.data_type);
    }

    // Preverimo, če se ujemata (imena in tipi)
    assert_eq!(dynamic_schema.columns.len(), 2);
    assert_eq!(dynamic_schema.columns[0].name, "user_id");
    assert_eq!(dynamic_schema.columns[1].name, "msg");
    
    println!("\n[Test Results]");
    println!("Catalog integrity check: PASSED");
    
    let next_oid = CatalogManager::find_next_avalible_oid(sm.clone(), tm.clone());
    println!("Next available OID in system: {}", next_oid);

    assert_eq!(count, 30, "Concurrency test failed: row count mismatch!");





    // ==========================================================
    // 7. VERBOZEN MVCC DROP TABLE TEST (Z obstoječimi metodami)
    // ==========================================================
    println!("\n==================================================");
    println!("[MVCC BUG HUNT] Začenjam preverjanje DROP TABLE...");
    println!("==================================================");

    // --- KORAK A: Izvedba DROP TABLE ---
    let drop_xid = tm.begin();
    println!("[Korak A] Začeta DROP transakcija z XID: {}", drop_xid);
    
    let drop_success = cm.drop_table(drop_xid, table_name);
    println!("[Korak A] cm.drop_table() izveden. Rezultat: {}", drop_success);
    
    // --- KORAK B: Commit drop transakcije ---
    println!("[Korak B] Potrjujem (commit) DROP transakcijo (XID {})...", drop_xid);
    tm.commit(drop_xid);

    // --- KORAK C: Testiranje vidnosti preko CatalogManagera (Kot HTTP server) ---
    println!("\n[Korak C] Simulacija NOVEGA HTTP zahtevka...");
    
    // Ker tvoj CatalogManager v get_schema ali iskanju interne sheme uporablja sistemske skane,
    // bova takoj videla, ali se tabela še vedno uspešno naloži iz katalogov.
    
    println!("[Korak C] Kličem cm.get_schema({}) po izvedenem DROP-u...", msg_table_oid);
    
    // Uloviva trenutno stanje preko cm vmesnika
    let post_drop_schema = cm.get_schema(msg_table_oid);
    
    println!("[Diagnostika] Število stolpcev vrnjenih iz katalogov: {}", post_drop_schema.columns.len());
    
    // --- KORAK D: Neposredni skan tabele (Če jo sm za nazaj sploh še najde) ---
    println!("\n[Korak D] Preverjam neposredni nizkonivojski skan preko Storage Managera...");
    let mut table_still_accessible = false;
    let mut rows_found_after_drop = 0;

    // Poskusimo dobiti ročico do tabele, da vidimo, če jo je StorageManager sploh odstranil
    let table_handle = sm.get_table(msg_table_oid);
    
    // Zaženemo skan z povsem novo transakcijo, da vidimo, če se podatki še berejo
    let check_xid = tm.begin();
    let scan_after_drop = HeapScan::new(bpm.clone(), table_handle, tm.clone());
    
    for _tuple in scan_after_drop {
        rows_found_after_drop += 1;
        table_still_accessible = true;
    }
    tm.commit(check_xid);

    println!("-> Najdenih vrstic v 'messages' datoteki po izbrisu: {}", rows_found_after_drop);

    // ==========================================================
    // KONČNA ANALIZA IN SKLEP
    // ==========================================================
    println!("\n==================================================");
    println!("[ANALIZA REZULTATOV]");
    println!("==================================================");
    
    if post_drop_schema.columns.len() > 0 {
        println!("❌ NAPAKA ULOVLJENA: Tabela je ŠE VEDNO VIDNA v sistemskih katalogih!");
        println!("   -> Čeprav je bil DROP potrjen, cm.get_schema še vedno najde definicijo tabele.");
        println!("   -> Vzrok: HeapScan znotraj CatalogManagera ne upošteva t_xmax statusa v CLOG-u.");
    } else {
        println!("✅ KATALOGI SO ČISTI: CatalogManager ne vidi več sheme za to tabelo.");
    }

    if table_still_accessible && rows_found_after_drop > 0 {
        println!("⚠️ OPOZORILO: Datoteka ali podatki na straneh so še vedno berljivi (Najdeno {} vrstic).", rows_found_after_drop);
        println!("   -> To je pričakovano, če še nisi počistil datoteke iz diska, vendar");
        println!("      sistemski katalogi (rg_class) je NE BI SMELI več kazati navzven.");
    }
    println!("==================================================");

    bpm.flush_all();














}
