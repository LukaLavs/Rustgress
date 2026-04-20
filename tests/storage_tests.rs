#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use rustgress::storage::buffer::manager::BufferPoolManager;
    use rustgress::storage::manager::StorageManager;
    use rustgress::access::transaction::manager::TransactionManager;
    use rustgress::catalog::manager::CatalogManager;
    use rustgress::catalog::schema::{Schema, Column};
    use rustgress::catalog::types::{DataType, Value};
    use rustgress::access::heap::heap_access::HeapAccess;
    use rustgress::access::heap::scan::HeapScan;

    #[test]
    fn test_database_complete_system_integration() {
        // ==========================================================
        // 1. SETUP (Identično kot v main)
        // ==========================================================
        let bpm = Arc::new(BufferPoolManager::new(50));
        let sm = Arc::new(StorageManager::new(bpm.clone()));
        let tm = Arc::new(TransactionManager::new());
        let cm = Arc::new(CatalogManager::new(sm.clone(), tm.clone()));
        
        cm.bootstrap_system_catalogs();

        // ==========================================================
        // 2. CONCURRENCY & BASIC INSERT (Iz tvojega maina)
        // ==========================================================
        let schema_msg = Arc::new(Schema::new(vec![
            Column { name: "user_id".to_string(), data_type: DataType::Integer },
            Column { name: "msg".to_string(), data_type: DataType::Varchar(100) },
        ]));

        let xid_setup = tm.begin();
        let msg_table_oid = cm.create_table(xid_setup, "messages_test", 0, &schema_msg);
        tm.commit(xid_setup);
       
        let mut handles = vec![];
        for user_id in 1..=3 {
            let sm_c = sm.clone();
            let tm_c = tm.clone();
            let schema_c = schema_msg.clone();
            
            let handle = thread::spawn(move || {
                let xid = tm_c.begin();
                for i in 1..=10 {
                    let mut tuple = schema_c.pack(vec![
                        Value::Integer(user_id as i32),
                        Value::Varchar(format!("Msg {} from user {}", i, user_id)),
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

        let table_handle = sm.get_table(msg_table_oid);
        let scan = HeapScan::new(bpm.clone(), table_handle, tm.clone());
        assert_eq!(scan.count(), 30, "Row count mismatch in concurrency test");

        // ==========================================================
        // 3. CATALOG LOOKUP (get_schema test)
        // ==========================================================
        let dynamic_schema = cm.get_schema(msg_table_oid);
        assert_eq!(dynamic_schema.columns.len(), 2);
        assert_eq!(dynamic_schema.columns[0].name, "user_id");
        
        // ==========================================================
        // 4. NULL BITMAP TEST
        // ==========================================================
        let schema_null = Schema::new(vec![
            Column { name: "c1".to_string(), data_type: DataType::Integer },
            Column { name: "c2".to_string(), data_type: DataType::Integer },
        ]);
        
        let xid_n = tm.begin();
        let null_oid = cm.create_table(xid_n, "null_table", 0, &schema_null);
        
        let row_nulls = vec![Value::Null, Value::Null];
        let mut n_tuple = schema_null.pack(row_nulls.clone());
        HeapAccess::insert(sm.clone(), xid_n, null_oid, &mut n_tuple);
        tm.commit(xid_n);

        let n_scan = HeapScan::new(bpm.clone(), sm.get_table(null_oid), tm.clone());
        let res_null = n_scan.last().unwrap();
        assert_eq!(schema_null.unpack_from_tuple(&res_null), row_nulls);

        // ==========================================================
        // 5. PAGE OVERFLOW TEST
        // ==========================================================
        let schema_big = Schema::new(vec![Column { name: "b".to_string(), data_type: DataType::Varchar(1000) }]);
        let xid_b = tm.begin();
        let big_oid = cm.create_table(xid_b, "big_table", 0, &schema_big);
        
        for _ in 0..20 {
            let mut b_tuple = schema_big.pack(vec![Value::Varchar("A".repeat(600))]);
            HeapAccess::insert(sm.clone(), xid_b, big_oid, &mut b_tuple);
        }
        tm.commit(xid_b);

        let big_handle = sm.get_table(big_oid);
        assert!(big_handle.read().unwrap().num_pages() > 1, "Should have multiple pages");

        // ==========================================================
        // 6. FINAL SYNC
        // ==========================================================
        bpm.flush_all();
    }
}