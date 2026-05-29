use std::sync::{Arc, Mutex};
use crate::utils::adt::datatype::{DataType};
use crate::utils::adt::integer::IntegerType;
use crate::storage::manager::StorageManager;
use crate::access::transaction::manager::TransactionManager;
use crate::access::heap::access::HeapAccess;
use crate::access::tuple::desc::{TupleDescriptor, Column};
use crate::common::constants::{
    RG_ATTRIBUTE_OID, RG_CLASS_OID, RG_TYPE_OID, RG_NAMESPACE_OID,
    USER_XID_START
};
use crate::storage::disk::manager::Table;
use crate::catalog::catalogs::traits::RGSomething;
use crate::catalog::catalogs::{
    rg_class::RGClass,
    rg_attribute::RGAttribute,
    rg_type::RGType,
    rg_namespace::RGNamespace,
};
use crate::access::heap::scan::HeapScan;
use crate::common::types::RowId;
use crate::access::transaction::context::{set_current_xid, clear_current_xid};
use crate::utils::debug::errors::{AccessError, LockError};

pub struct CatalogManager {
    pub storage: Arc<StorageManager>,
    pub tm: Arc<TransactionManager>,
    pub next_oid: Mutex<u32>,
}

impl CatalogManager {
    pub fn new(storage: Arc<StorageManager>, tm: Arc<TransactionManager>) -> Result<Self, LockError> {
        let initial_oid = Self::find_next_avalible_oid(storage.clone(), tm.clone())?;
        Ok(Self {
            storage,
            tm,
            next_oid: Mutex::new(initial_oid),
        })
    }
    /// Scans the rg_class catalog to find the maximum OID currently in use, it returns the next one.
    pub fn find_next_avalible_oid(storage: Arc<StorageManager>, tm: Arc<TransactionManager>) -> Result<u32, LockError> {
        let path = format!("data/{}", RG_CLASS_OID);
        if !std::path::Path::new(&path).exists() { // in bootstrap phase.
            return Ok(USER_XID_START);
        }
        let rg_class_table = storage
            .get_system_table_with_recovery(RG_CLASS_OID);
        let bpm = storage.get_bpm();
        let scan = HeapScan::new(bpm, rg_class_table, tm)?;
        let schema = RGClass::get_descriptor();
        let mut max_oid = USER_XID_START;
        for tuple in scan {
            let values = schema.unpack_from_tuple(&tuple);
            // in RGClass oid is first column (zero indexed)
            if let Some(oid_val) = values.get(0) {
                if cfg!(debug_assertions) {println!("CatalogManager: Inspecting OID from rg_class: {:?}", oid_val)};
                if let Some(oid) = oid_val.as_native::<IntegerType>(){
                    if cfg!(debug_assertions) {println!("CatalogManager: Found existing OID in rg_class: {}", oid)};
                    if oid as u32 >= max_oid {
                        max_oid = (oid as u32) + 1;
                    }
                }
            }
        }
        if cfg!(debug_assertions) {println!("CatalogManager: Next available OID determined to be: {}", max_oid)};
        Ok(max_oid)
    }
    fn generate_next_oid(&self) -> Result<u32, AccessError> {
        let mut lock = self.next_oid.lock().map_err(|_| LockError)?;
        let oid = *lock;
        *lock += 1;
        Ok(oid)
    }
    /// returns new oid
    pub fn create_table(&self, name: &str, special_size: u16, schema: &TupleDescriptor) -> Result<u32, AccessError> {
        let new_oid = self.generate_next_oid()?;
        if self.get_table_oid(name).is_ok() {
            return Err(AccessError::DuplicatedTableNames);
        }
        Table::create(new_oid, special_size)?;
        {
            let table_handle = self.storage.get_table(new_oid)?;
            let mut table = table_handle.write().map_err(|_| LockError)?;
            table.extend(0)?; // TODO: hardcoded for now
        }
        let rg_class_schema = RGClass::get_descriptor();
        let rg_attribute_schema = RGAttribute::get_descriptor();

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
            RG_CLASS_OID,
            &mut class_tuple
        )?;

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
                RG_ATTRIBUTE_OID,
                &mut attr_tuple
            )?;
        }

        Ok(new_oid)
    }
}

impl CatalogManager {
    pub fn drop_table(&self, table_name: &str) -> Result<bool, AccessError> {
        let table_oid = match self.get_table_oid(table_name) {
            Ok(oid) => oid as u32,
            Err(_) => return Ok(false), // table didn't exist, couldn't drop it
        };
        let bpm = self.storage.get_bpm();
        // DELETE FROM rg_class
        let class_table = self.storage
            .get_system_table_with_recovery(RG_CLASS_OID);
        let mut scan = HeapScan::new(bpm.clone(), class_table, self.tm.clone())?;
        if let Some(tuple) = scan
            .find(|t| RGClass::from_tuple(t).oid as u32 == table_oid) 
        {
            HeapAccess::delete(self.storage.clone(), RG_CLASS_OID, tuple.header.get_rid())?;
        }
        // DELETE FROM rg_attribute
        let attr_table = self.storage
            .get_system_table_with_recovery(RG_ATTRIBUTE_OID);
        HeapScan::new(bpm.clone(), attr_table, self.tm.clone())?
            .filter(|t| RGAttribute::from_tuple(t).attrelid as u32 == table_oid)
            .try_for_each(|t| {
                HeapAccess::delete(self.storage.clone(), RG_ATTRIBUTE_OID, t.header.get_rid())?;
                Ok::<(), AccessError>(()) // Povej closure-u, kaj vrača ob uspehu
            })?;
        // REMOVE file from disk
        bpm.flush_all()?;
        bpm.evict_table_pages(table_oid)?;
        let _ = std::fs::remove_file(format!("data/{}", table_oid));

        Ok(true)
    }
    
    pub fn drop_table_old(&self, table_name: &str) -> Result<bool, AccessError> {
        let table_oid = match self.get_table_oid(table_name) {
            Ok(oid) => oid as u32,
            Err(_) => return Ok(false), // table didn't exist, couldn't drop it
        };
        let bpm = self.storage.get_bpm();
        let class_table = self.storage
            .get_system_table_with_recovery(RG_CLASS_OID);
        let class_rid_to_delete = HeapScan::new(bpm.clone(), class_table.clone(), self.tm.clone())?
            .find(|t| RGClass::from_tuple(t).oid as u32 == table_oid)
            .map(|tuple| tuple.header.get_rid());
        if let Some(rid) = class_rid_to_delete {
            HeapAccess::delete(self.storage.clone(),  RG_CLASS_OID, rid)?;
        }
        let attr_table = self.storage
            .get_system_table_with_recovery(RG_ATTRIBUTE_OID);
        
        // Iterator se tukaj zažene in POPOLNOMA zaključi, preden karkoli brišemo
        let attr_rids_to_delete: Vec<RowId> = HeapScan::new(bpm.clone(), attr_table.clone(), self.tm.clone())?
            .filter(|t| RGAttribute::from_tuple(t).attrelid as u32 == table_oid)
            .map(|t| t.header.get_rid())
            .collect(); // <--- KLJUČNO: collect() posesa vse v spomin in sprosti iterator!
        // Šele ko je HeapScan popolnoma mrtev in zaprt, varno pobrišemo atribute
        println!("DropFUNC: attr_rids_to_delete: {:?}", attr_rids_to_delete);
        for rid in attr_rids_to_delete {
            HeapAccess::delete(self.storage.clone(), RG_ATTRIBUTE_OID, rid)?;
        }
        bpm.flush_all()?; // Vpiše posodobljena rg_class in rg_attribute na disk
        let _ = std::fs::remove_file(format!("data/{}", table_oid));
        bpm.evict_table_pages(table_oid)?;
        Ok(true)
    }
}

impl CatalogManager {
    /// Retrieves the schema of a table given its OID by scanning the rg_attribute catalog.
    pub fn get_schema(&self, table_oid: u32) -> Result<TupleDescriptor, AccessError> {
        let attr_table = self.storage
            .get_system_table_with_recovery(RG_ATTRIBUTE_OID);
        let scan = HeapScan::new(self.storage.get_bpm(), attr_table, self.tm.clone())?;
        let mut attributes: Vec<RGAttribute> = scan
            .map(|tuple| RGAttribute::from_tuple(&tuple))
            .filter(|attr| attr.attrelid as u32 == table_oid)
            .collect();
        attributes.sort_by_key(|a| a.attnum);
        let columns = attributes
            .into_iter()
            .map(|attr| Column {
                name: attr.attname,
                data_type: DataType::from_oid(attr.atttypid as u32),
            })
            .collect();
        Ok(TupleDescriptor::new(columns))
    }
    pub fn get_table_oid(&self, table_name: &str) -> Result<u32, AccessError> { // TODO: check if it is safe to replace with get_table_oid
        let table_handle = self.storage
            .get_system_table_with_recovery(RG_CLASS_OID);
        let scan = HeapScan::new(self.storage.get_bpm(), table_handle, self.tm.clone())?;
        let table_oid = scan
            .map(|tuple| RGClass::from_tuple(&tuple))
            .find(|class_entry| class_entry.relname == table_name)
            .map(|class_entry| class_entry.oid as u32)
            .ok_or_else(|| AccessError::TableNotFound(table_name.to_string()))?;
        Ok(table_oid)
    }
}

impl CatalogManager {
    pub fn bootstrap_system_catalogs(&self) -> Result<bool, AccessError> {
        if std::path::Path::new(&format!("data/{}", RG_CLASS_OID)).exists() {
            if cfg!(debug_assertions) { println!("Sistem catalogs exist, skipping bootstrap."); }
            return Ok(false);
        }
        println!("Bootstraping system catalogs ...");
        std::fs::create_dir_all("data").map_err(|_| AccessError::DataFolderCreationFailed)?;
        let xid = self.tm.begin()?;
        set_current_xid(xid);

        Self::bootstrap_rg_class(self.storage.clone())?;
        Self::bootstrap_rg_attribute(self.storage.clone())?;
        Self::bootstrap_rg_type(self.storage.clone())?;
        Self::bootstrap_rg_namespace(self.storage.clone())?;

        self.tm.commit(xid)?;
        clear_current_xid();
        self.tm.flush()?;
        println!("Bootstrap finalized.");
        return Ok(true);
    }

    pub fn recover_system_catalog(storage: Arc<StorageManager>, oid: u32) -> Result<(), AccessError> {
        match oid {
            RG_CLASS_OID => Self::bootstrap_rg_class(storage.clone()),
            RG_ATTRIBUTE_OID => Self::bootstrap_rg_attribute(storage.clone()),
            RG_TYPE_OID => Self::bootstrap_rg_type(storage.clone()),
            RG_NAMESPACE_OID => Self::bootstrap_rg_namespace(storage.clone()),
            _ => Err(AccessError::TableNotFound(format!("System catalog with OID {} not found for recovery", oid))),
        }
    }

    fn bootstrap_rg_class(storage: Arc<StorageManager>) 
        -> Result<(), AccessError> {
        Table::create(RG_CLASS_OID, 0)?;
        let rg_class_schema = RGClass::get_descriptor();
        let class_entries = [
            (RG_CLASS_OID, "rg_class", 7), // rg_class  has 7 columns
            (RG_ATTRIBUTE_OID, "rg_attribute", 5),
            (RG_TYPE_OID, "rg_type", 4),
            (RG_NAMESPACE_OID, "rg_namespace", 4),
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
                storage.clone(),
                RG_CLASS_OID, 
                &mut tuple
            )?;
        }
        Ok(())
    }

    fn bootstrap_rg_attribute(storage: Arc<StorageManager>) 
        -> Result<(), AccessError> {
        Table::create(RG_ATTRIBUTE_OID, 0)?;
        let rg_class_schema = RGClass::get_descriptor();
        let rg_attribute_schema = RGAttribute::get_descriptor();
        let rg_type_schema = RGType::get_descriptor();
        let rg_namespace_schema = RGNamespace::get_descriptor();
        for (i, col) in rg_class_schema.columns.iter().enumerate() {
            let mut tuple = RGAttribute {
                attrelid: RG_CLASS_OID as i32,
                attname: col.name.clone(),
                atttypid: col.data_type.get_oid() as i32,
                attnum: (i + 1) as i32, // 1-based
                attlen: col.data_type.get_byte_len(),
            }.make_tuple(&rg_attribute_schema);
            HeapAccess::insert(storage.clone(), RG_ATTRIBUTE_OID, &mut tuple)?;
        }
        for (i, col) in rg_attribute_schema.columns.iter().enumerate() {
            let mut tuple = RGAttribute {
                attrelid: RG_ATTRIBUTE_OID as i32,
                attname: col.name.clone(),
                atttypid: col.data_type.get_oid() as i32,
                attnum: (i + 1) as i32, // 1-based
                attlen: col.data_type.get_byte_len(),
            }.make_tuple(&rg_attribute_schema);
            HeapAccess::insert(storage.clone(), RG_ATTRIBUTE_OID, &mut tuple)?;
        }

        for (i, col) in rg_type_schema.columns.iter().enumerate() {
            let mut tuple = RGAttribute {
                attrelid: RG_TYPE_OID as i32,
                attname: col.name.clone(),
                atttypid: col.data_type.get_oid() as i32,
                attnum: (i + 1) as i32, // 1-based
                attlen: col.data_type.get_byte_len(),
            }.make_tuple(&rg_attribute_schema);
            HeapAccess::insert(storage.clone(), RG_ATTRIBUTE_OID, &mut tuple)?;
        }

        for (i, col) in rg_namespace_schema.columns.iter().enumerate() {
            let mut tuple = RGAttribute {
                attrelid: RG_NAMESPACE_OID as i32,
                attname: col.name.clone(),
                atttypid: col.data_type.get_oid() as i32,
                attnum: (i + 1) as i32, // 1-based
                attlen: col.data_type.get_byte_len(),
            }.make_tuple(&rg_attribute_schema);
            HeapAccess::insert(storage.clone(), RG_ATTRIBUTE_OID, &mut tuple)?;
        }
        Ok(())
    }

    fn bootstrap_rg_type(storage: Arc<StorageManager>) -> Result<(), AccessError> {
        // Definiramo seznam vseh osnovnih tipov, ki jih sistem podpira
        Table::create(RG_TYPE_OID, 0)?;
        let rg_type_schema = RGType::get_descriptor();
        let type_definitions = DataType::type_definitions();
        for (oid, name, len, byval) in type_definitions {
            let mut tuple = RGType {
                oid: oid as i32,
                typname: name.to_string(),
                typlen: len as i32,
                typbyval: byval,
            }.make_tuple(&rg_type_schema);
            HeapAccess::insert(
                storage.clone(),
                RG_TYPE_OID, 
                &mut tuple
            )?;
        }
        Ok(())
    }

    fn bootstrap_rg_namespace(storage: Arc<StorageManager>) 
        -> Result<(), AccessError> {
        Table::create(RG_NAMESPACE_OID, 0)?;
        let rg_namespace_schema = RGNamespace::get_descriptor();
        let mut tuple = RGNamespace {
            oid: RG_NAMESPACE_OID as i32,
            nspname: "public".to_string(),
            nspowner: 0, // TODO: hardcoded for now
            nspacl: 0,   // TODO: hardcoded for now
        }.make_tuple(&rg_namespace_schema);
        HeapAccess::insert(
            storage.clone(),
            RG_NAMESPACE_OID, 
            &mut tuple
        )?;
        Ok(())
    }
}

/// Error recovery for system catalogs. If error could not be reverted must panic.
pub trait StorageCatalogExt {
    fn get_system_table_with_recovery(&self, catalog_oid: u32) -> Arc<std::sync::RwLock<Table>>;
}

impl StorageCatalogExt for Arc<StorageManager> {
    fn get_system_table_with_recovery(&self, catalog_oid: u32) -> Arc<std::sync::RwLock<Table>> {
        match self.get_table(catalog_oid) {
            Ok(table) => table,
            Err(_err) => {
                // Here we could theoretically handle recovery, but we would need to check
                // rg_attributes also to make sure we obtain as much functionality as possible
                // and to not cause further errors.
                panic!("Critical: Failed to access system catalog with OID {}. Attempting recovery...", catalog_oid);
                // if cfg!(debug_assertions) {
                //     println!("CatalogExt: System catalog {} missing, triggering recovery...", catalog_oid);
                // }
                // CatalogManager::recover_system_catalog(self.clone(), catalog_oid)
                //     .expect(&format!("Critical: Failed to recover system catalog {}", catalog_oid));
                // self.get_table(catalog_oid)
                //     .expect(&format!("Critical: Failed to retrieve system catalog {} after recovery", catalog_oid))
            }
        }
    }
}