use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use crate::storage::buffer::manager::BufferPoolManager;
use crate::storage::disk::manager::Table;

/// StorageManager is responsible for managing access to tables, 
/// preventing multiple concurrent write accesses.
pub struct StorageManager {
    bpm: Arc<BufferPoolManager>,
    tables: RwLock<HashMap<u32, Arc<RwLock<Table>>>>, // map from table oid to Table instance
}

impl StorageManager {
    pub fn new(bpm: Arc<BufferPoolManager>) -> Self {
        Self {
            bpm,
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Get Table access, if already open return existing instance.
    pub fn get_table(&self, oid: u32) -> Arc<RwLock<Table>> {
        {   // already exist, return it 
            let tables_read = self.tables.read().unwrap();
            if let Some(table) = tables_read.get(&oid) {
                return Arc::clone(table);
            }
        }
        let mut tables_write = self.tables.write().unwrap();
        tables_write.entry(oid)
            .or_insert_with(|| Arc::new(RwLock::new(Table::open(oid))))
            .clone()
    }

    pub fn get_bpm(&self) -> Arc<BufferPoolManager> {
        Arc::clone(&self.bpm)
    }
}