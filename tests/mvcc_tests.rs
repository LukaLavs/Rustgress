use std::sync::Arc;
use std::fs;
use rustgress::storage::manager::StorageManager;
use rustgress::access::heap::access::HeapAccess;
use rustgress::storage::buffer::manager::BufferPoolManager;
use rustgress::access::transaction::manager::TransactionManager;
use rustgress::catalog::manager::CatalogManager;
use rustgress::access::tuple::desc::{TupleDescriptor, Column};
use rustgress::catalog::types::{DataType, Value};
use rustgress::access::heap::scan::HeapScan;
use rustgress::common::constants::*;
use rustgress::catalog::catalogs::rg_class::RGClass;
use rustgress::catalog::catalogs::traits::RGSomething;

#[test]
fn test_database_mvcc_full_cycle() {
    // --- 1. SETUP ---
    // Use a specific test directory to avoid any conflict with the main 'data' folder
    let test_dir = "data_mvcc_test";
    
    if fs::metadata(test_dir).is_ok() {
        fs::remove_dir_all(test_dir).ok();
    }
    // Small sleep to let the OS release file locks from previous runs
    std::thread::sleep(std::time::Duration::from_millis(50));
    fs::create_dir_all(test_dir).expect("Failed to create test directory");

    let bpm = Arc::new(BufferPoolManager::new(100));

    // If your StorageManager takes a path, pass it here. 
    // If it's hardcoded to "data/", change this test to use "data/":
    let sm = Arc::new(StorageManager::new(bpm.clone())); 
    let tm = Arc::new(TransactionManager::new());
    let cm = Arc::new(CatalogManager::new(sm.clone(), tm.clone()));

    // Verify directory exists just before bootstrap
    assert!(fs::metadata("data").is_ok(), "Data directory disappeared before bootstrap!");

    println!("Starting bootstrap...");
    cm.bootstrap_system_catalogs();
    
    bpm.flush_all();

    // --- 2. BASIC INSERT & SCAN TEST ---
    {
        let schema = TupleDescriptor::new(vec![Column { name: "id".to_string(), data_type: DataType::Integer }]);
        let xid = tm.begin();
        let table_oid = cm.create_table(xid, "table_basic", 0, &schema);
        
        let mut tuple = schema.pack(vec![Value::Integer(42)]);
        HeapAccess::insert(sm.clone(), xid, table_oid, &mut tuple);
        tm.commit(xid);

        let table = sm.get_table(table_oid);
        let scan = HeapScan::new(bpm.clone(), table, tm.clone());
        assert_eq!(scan.count(), 1, "Basic scan should find 1 committed row");
    }

    // --- 3. DIRTY READ PREVENTION TEST ---
    {
        let schema = TupleDescriptor::new(vec![Column { name: "val".to_string(), data_type: DataType::Integer }]);
        let xid_setup = tm.begin();
        let table_oid = cm.create_table(xid_setup, "table_dirty", 0, &schema);
        tm.commit(xid_setup);

        let xid_a = tm.begin();
        let mut tuple = schema.pack(vec![Value::Integer(100)]);
        HeapAccess::insert(sm.clone(), xid_a, table_oid, &mut tuple);

        let table = sm.get_table(table_oid);
        let scan = HeapScan::new(bpm.clone(), table.clone(), tm.clone());
        assert_eq!(scan.count(), 0, "Dirty read detected: saw uncommitted tuple");

        tm.commit(xid_a);
        let scan_after = HeapScan::new(bpm.clone(), table, tm.clone());
        assert_eq!(scan_after.count(), 1, "Tuple should be visible after commit");
    }

    // --- 4. SNAPSHOT ISOLATION TEST ---
    {
        let schema = TupleDescriptor::new(vec![Column { name: "val".to_string(), data_type: DataType::Integer }]);
        let xid_setup = tm.begin();
        let table_oid = cm.create_table(xid_setup, "table_snap", 0, &schema);
        tm.commit(xid_setup);

        let table = sm.get_table(table_oid);
        // Start a scan/transaction now to capture the current snapshot (empty table)
        let scan = HeapScan::new(bpm.clone(), table.clone(), tm.clone());

        let xid_write = tm.begin();
        let mut tuple = schema.pack(vec![Value::Integer(200)]);
        HeapAccess::insert(sm.clone(), xid_write, table_oid, &mut tuple);
        tm.commit(xid_write);

        assert_eq!(scan.count(), 0, "Snapshot isolation failure: scan saw a future commit");
    }

    // --- 5. DELETE & VISIBILITY TEST ---
    {
        let schema = TupleDescriptor::new(vec![Column { name: "val".to_string(), data_type: DataType::Integer }]);
        let xid_setup = tm.begin();
        let table_oid = cm.create_table(xid_setup, "table_del", 0, &schema);
        tm.commit(xid_setup);

        let xid_1 = tm.begin();
        let mut tuple = schema.pack(vec![Value::Integer(500)]);
        let rid = HeapAccess::insert(sm.clone(), xid_1, table_oid, &mut tuple);
        tm.commit(xid_1);

        let xid_2 = tm.begin();
        HeapAccess::delete(sm.clone(), xid_2, table_oid, rid);

        let table = sm.get_table(table_oid);
        {
            let scan = HeapScan::new(bpm.clone(), table.clone(), tm.clone());
            assert_eq!(scan.count(), 1, "Tuple should remain visible until delete is committed");
        }

        tm.commit(xid_2);
        let scan_final = HeapScan::new(bpm.clone(), table, tm.clone());
        assert_eq!(scan_final.count(), 0, "Tuple should be invisible after delete commit");
    }

    // --- 6. CATALOG PERSISTENCE (CTID) TEST ---
    {
        let xid = tm.begin();
        let table_oid = cm.create_table(xid, "table_ctid", 0, &TupleDescriptor::new(vec![
            Column { name: "a".to_string(), data_type: DataType::Integer }
        ]));
        tm.commit(xid);

        let class_table = sm.get_table(RG_CLASS_OID);
        let scan = HeapScan::new(bpm.clone(), class_table, tm.clone());
        
        let mut found = false;
        for tuple in scan {
            let entry = RGClass::from_tuple(&tuple);
            if entry.oid as u32 == table_oid {
                found = true;
                assert!(tuple.header.t_ctid_slot >= 1, "CTID slot must be valid");
                let rid = tuple.header.get_rid();
                assert_eq!(rid.page_id, tuple.header.t_ctid_page, "CTID page index mismatch");
            }
        }
        assert!(found, "Table OID not found in rg_class catalog");
    }

    // --- 7. FINAL CLEANUP ---
    bpm.flush_all();
}