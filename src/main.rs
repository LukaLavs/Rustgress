use std::sync::{Arc};
use std::thread;
use rustgress::storage::buffer::manager::BufferPoolManager;
use rustgress::storage::manager::StorageManager;
use rustgress::access::transaction::manager::TransactionManager;
use rustgress::catalog::manager::CatalogManager;
use rustgress::catalog::schema::{Schema, Column};
use rustgress::catalog::types::{DataType, Value};
use rustgress::access::heap::heap_access::HeapAccess;
use rustgress::access::heap::scan::HeapScan;


fn main() {
    // 1. Setup shared system components
    let bpm = Arc::new(BufferPoolManager::new(50));
    let sm = Arc::new(StorageManager::new(bpm.clone()));
    let tm = Arc::new(TransactionManager::new());
    let cm = Arc::new(CatalogManager::new(sm.clone(), tm.clone()));
    
    cm.bootstrap_system_catalogs();

    // 2. Define a schema
    let schema = Arc::new(Schema::new(vec![
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
                ]);
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
        let data = schema.unpack_from_tuple(&tuple);
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

    bpm.flush_all(); 
}