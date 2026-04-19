use std::sync::{Arc, Mutex};
use crate::storage::manager::StorageManager;
use crate::access::transaction::manager::TransactionManager;
use crate::access::heap::heap_access::HeapAccess;
use crate::catalog::schema::Schema;
use crate::common::constants::{RG_ATTRIBUTE_OID, RG_CLASS_OID, USER_XID_START};
use crate::storage::disk::manager::Table;
use crate::catalog::catalogs::traits::RGSomething;
use crate::catalog::catalogs::{
    rg_class::RGClass,
    rg_attribute::RGAttribute,
};
use crate::access::heap::scan::HeapScan;

pub struct CatalogManager {
    pub storage: Arc<StorageManager>,
    pub tm: Arc<TransactionManager>,
    pub next_oid: Mutex<u32>,
}

impl CatalogManager {
    pub fn new(storage: Arc<StorageManager>, tm: Arc<TransactionManager>) -> Self {
        let initial_oid = Self::find_next_avalible_oid(storage.clone(), tm.clone());
        Self {
            storage,
            tm,
            next_oid: Mutex::new(initial_oid),
        }
    }
    pub fn find_next_avalible_oid(storage: Arc<StorageManager>, tm: Arc<TransactionManager>) -> u32 {
        let rg_class_oid = RG_CLASS_OID;
        let path = format!("data/{}", rg_class_oid);
        if !std::path::Path::new(&path).exists() { // in bootstrap phase.
            return USER_XID_START;
        }
        let rg_class_table = storage.get_table(rg_class_oid);
        let bpm = storage.get_bpm();
        let scan = HeapScan::new(bpm, rg_class_table, tm);
        let schema = RGClass::get_schema();
        let mut max_oid = USER_XID_START;
        for tuple in scan {
            let values = schema.unpack_from_tuple(&tuple);
            // in RGClass oid is first column (zero indexed)
            if let Some(oid_val) = values.get(0) {
                println!("Inspecting OID from rg_class: {:?}", oid_val);
                if let Some(oid) = oid_val.as_i32() {
                    println!("Found existing OID in rg_class: {}", oid);
                    if oid as u32 >= max_oid {
                        max_oid = (oid as u32) + 1;
                    }
                }
            }
        }
        println!("Next available OID determined to be: {}", max_oid);
        max_oid
    }
    fn generate_next_oid(&self) -> u32 {
        let mut lock = self.next_oid.lock().unwrap();
        let oid = *lock;
        *lock += 1;
        oid
    }
    pub fn create_table(&self, xid: u64, name: &str, special_size: u16, schema: &Schema) -> u32 {
        let new_oid = self.generate_next_oid();
        Table::create(new_oid, special_size);
        {
            let table_handle = self.storage.get_table(new_oid);
            let mut table = table_handle.write().unwrap();
            table.extend(0); // TODO: hardcoded for now
        }
        let rg_class_schema = RGClass::get_schema();
        let rg_attribute_schema = RGAttribute::get_schema();

        let mut class_tuple = RGClass {
            oid: new_oid as i32,
            relname: name.to_string(),
            relnamespace: 0,  // public schema
            relpages: 1,     // hardcoded for now, TODO.
            reltuples: 0.0, // empty at creation
            relspecial: 0, // hardcoded for now, TODO.
            relnatts: schema.columns.len() as i32,
        }.make_tuple(&rg_class_schema);
        HeapAccess::insert(
            self.storage.clone(),
            xid,
            RG_CLASS_OID,
            &mut class_tuple
        );

        // WRITE TO rg_attribute
        for (i, col) in schema.columns.iter().enumerate() {
            let mut attr_tuple = RGAttribute {
                attrelid: new_oid as i32,
                attname: col.name.clone(),
                atttypid: col.data_type.get_oid() as i32,
                attnum: (i + 1) as i32, // position of column in the table, 1-based
                attlen: col.data_type.get_byte_len(),
            }.make_tuple(&rg_attribute_schema);
            HeapAccess::insert(
                self.storage.clone(),
                xid,
                RG_ATTRIBUTE_OID,
                &mut attr_tuple
            );
        }

        new_oid
    }
}


impl CatalogManager {
    pub fn bootstrap_system_catalogs(&self) {
        let rg_class_oid = RG_CLASS_OID;
        let rg_attribute_oid = RG_ATTRIBUTE_OID;
        if std::path::Path::new(&format!("data/{}", rg_class_oid)).exists() {
            println!("Sistem catalogs exist, skipping bootstrap.");
            return;
        }
        println!("Starting bootstrap of system catalogs...");
        std::fs::create_dir_all("data").expect("Folder data could not be created!");
        {
            Table::create(rg_class_oid, 0);
            Table::create(rg_attribute_oid, 0);
        }
        let xid = self.tm.begin();

        let rg_class_schema = RGClass::get_schema();
        let rg_attribute_schema = RGAttribute::get_schema();


        // WRITE TO rg_class
        let class_entries = [
            (RG_CLASS_OID, "rg_class", 7),      // rg_class  has 7 columns
            (RG_ATTRIBUTE_OID, "rg_attribute", 5), // rg_attribute has 5 columns
        ];
        for (oid, name, natts) in class_entries {
            let mut tuple = RGClass {
                oid: oid as i32,
                relname: name.to_string(),
                relnamespace: 0, // public schema
                relpages: 1,     // TODO: hardcoded, but ok.
                reltuples: 0.0,  // empty table at bootstrap
                relspecial: 0,
                relnatts: natts,
            }.make_tuple(&rg_class_schema);
            HeapAccess::insert(
                self.storage.clone(), 
                xid, 
                RG_CLASS_OID, 
                &mut tuple
            );
        }
        
        // WRITE TO rg_attribute
        for (i, col) in rg_class_schema.columns.iter().enumerate() {
            let mut tuple = RGAttribute {
                attrelid: rg_class_oid as i32,
                attname: col.name.clone(),
                atttypid: col.data_type.get_oid() as i32,
                attnum: (i + 1) as i32, // 1-based
                attlen: col.data_type.get_byte_len(),
            }.make_tuple(&rg_attribute_schema);
            HeapAccess::insert(self.storage.clone(), xid, rg_attribute_oid, &mut tuple);
        }
        for (i, col) in rg_attribute_schema.columns.iter().enumerate() {
            let mut tuple = RGAttribute {
                attrelid: rg_attribute_oid as i32,
                attname: col.name.clone(),
                atttypid: col.data_type.get_oid() as i32,
                attnum: (i + 1) as i32, // 1-based
                attlen: col.data_type.get_byte_len(),
            }.make_tuple(&rg_attribute_schema);
            HeapAccess::insert(self.storage.clone(), xid, rg_attribute_oid, &mut tuple);
        }
        self.tm.commit(xid);
        println!("Bootstrap finalized.");
    }
}