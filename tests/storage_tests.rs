#![allow(unused_imports)]
use rustgress::storage::page::header::PageHeaderData;
use rustgress::access::tuple::header::{
    ItemIdData,
};

use rustgress::catalog::schema::{Schema, Column};
use rustgress::catalog::types::{DataType, Value};
use rustgress::storage::disk::manager::Table;
use rustgress::catalog::catalogs::bootstrap_system_catalogs;
use rustgress::catalog::rg_class::RGClass;
use rustgress::catalog::rg_attribute::RGAttribute;
use rustgress::catalog::traits::RGSomething;
use std::sync::Arc;
use rustgress::storage::buffer::manager::BufferPoolManager;
use rustgress::access::heap::scan::HeapScan;
use rustgress::access::tuple::header::HeapTupleView;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::fs;

    fn setup(test_id: u32) {
        let _ = fs::create_dir_all("data"); 
        let path = format!("data/{}", test_id);
        let _ = fs::File::create(path).expect("Ni bilo mogoče pripraviti testne datoteke");
    }

    #[test]
    fn test_basic_insertion_and_retrieval() {
        setup(1234);
        let schema = Schema::new(vec![
            Column { name: "id".to_string(), data_type: DataType::Integer },
            Column { name: "tag".to_string(), data_type: DataType::Varchar(100) },
        ]);
        let mut table = Table::open(1234);
        let row = vec![Value::Integer(10), Value::Varchar("Standard".to_string())];
        
        table.insert_tuple(&schema.pack(row.clone()));
        
        let page = table.read_page(0);
        let view = page.get_tuple_view(1).expect("Tuple bi moral obstajati v slotu 1");
        assert_eq!(schema.unpack(&view), row);
    }

    #[test]
    fn test_null_bitmaps() {
        setup(1235);
        let schema = Schema::new(vec![
            Column { name: "c1".to_string(), data_type: DataType::Integer },
            Column { name: "c2".to_string(), data_type: DataType::Integer },
        ]);
        let mut table = Table::open(1235);
        let row_nulls = vec![Value::Null, Value::Null];
        
        table.insert_tuple(&schema.pack(row_nulls.clone()));

        // 1. Shrani stran v spremenljivko, da ostane živa
        let page = table.read_page(0); 
        
        // 2. Zdaj si view izposodi podatke iz 'page', ki bo živ do konca funkcije
        let view = page.get_tuple_view(1).expect("Tuple bi moral biti v slotu 1");
        
        assert_eq!(schema.unpack(&view), row_nulls);
    }

    #[test]
    fn test_page_overflow() {
        setup(1236);
        let schema = Schema::new(vec![Column { name: "data".to_string(), data_type: DataType::Varchar(1000) }]);
        let mut table = Table::open(1236);
        let large_row = vec![Value::Varchar("X".repeat(500))];

        // Vstavimo dovolj, da zapolnimo več kot eno 8KB stran
        for _ in 0..20 {
            table.insert_tuple(&schema.pack(large_row.clone()));
        }

        assert!(table.num_pages() > 1, "Tabela bi morala imeti več strani");
    }

    #[test]
    fn test_buffer_pool_scan() {
        setup(5000);
        let bpm = Arc::new(BufferPoolManager::new(5));
        let mut table = Table::open(5000);
        let schema = Schema::new(vec![Column { name: "val".to_string(), data_type: DataType::Integer }]);

        for i in 1..=100 {
            table.insert_tuple(&schema.pack(vec![Value::Integer(i)]));
        }

        let scan = HeapScan::new(bpm, &mut table);
        let count = scan.count();
        assert_eq!(count, 100);
    }

    #[test]
    fn test_cross_table_isolation() {
        setup(9999);
        let bpm = Arc::new(BufferPoolManager::new(10));
        let schema = Schema::new(vec![Column { name: "v".to_string(), data_type: DataType::Integer }]);
        
        let mut t1 = Table::open(1001);
        let mut t2 = Table::open(1002);
        
        t1.insert_tuple(&schema.pack(vec![Value::Integer(1)]));
        t2.insert_tuple(&schema.pack(vec![Value::Integer(2)]));

        let mut scan1 = HeapScan::new(bpm.clone(), &mut t1);
        let mut scan2 = HeapScan::new(bpm.clone(), &mut t2);

        let v1 = scan1.next().unwrap().data;
        let v2 = scan2.next().unwrap().data;

        assert_eq!(i32::from_le_bytes(v1[0..4].try_into().unwrap()), 1);
        assert_eq!(i32::from_le_bytes(v2[0..4].try_into().unwrap()), 2);
    }
}