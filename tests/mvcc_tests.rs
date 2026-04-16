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
// use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc;
use rustgress::storage::buffer::manager::BufferPoolManager;
use rustgress::access::heap::scan::HeapScan;
use rustgress::access::transaction::manager::TransactionManager;

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
        fn test_mvcc_visibility_committed() {
            // Pripravimo okolje
            setup(7000);
            let bpm = Arc::new(BufferPoolManager::new(10));
            let tm = Arc::new(TransactionManager::new(100));
            let mut table = Table::open(7000);
            let schema = Schema::new(vec![Column { name: "val".to_string(), data_type: DataType::Integer }]);

            // 1. Transakcija vstavlja podatek
            let xid = tm.begin();
            let mut tuple = schema.pack(vec![Value::Integer(42)]);
            tuple.header.t_xmin = xid as u32; // Nastavimo xmin na naš XID
            table.insert_tuple(&tuple);

            // 2. Pred COMMIT-om ustvarimo scan - ne bi smel videti ničesar (preprečevanje Dirty Read)
            {
                let scan_before = HeapScan::new(bpm.clone(), &mut table, tm.clone());
                assert_eq!(scan_before.count(), 0, "Scan ne bi smel videti ne-potrjenih podatkov");
            }

            // 3. POTRDIMO transakcijo
            tm.commit(xid);
            tm.flush();

            // 4. Po COMMIT-u ustvarimo nov scan - zdaj mora videti podatek
            let scan_after = HeapScan::new(bpm.clone(), &mut table, tm.clone());
            assert_eq!(scan_after.count(), 1, "Scan bi moral videti potrjene podatke");
        }

    #[test]
    fn test_mvcc_snapshot_isolation() {
        setup(7001);
        let bpm = Arc::new(BufferPoolManager::new(10));
        let tm = Arc::new(TransactionManager::new(200));
        
        // 1. Odpremo tabelo samo za vstavljanje
        let mut write_table = Table::open(7001);
        let schema = Schema::new(vec![Column { name: "val".to_string(), data_type: DataType::Integer }]);

        // 2. Ustvarimo scan na DRUGEM objektu iste tabele
        // To simulira drugo sejo. Ker oba kažeta na isti OID (isto datoteko), 
        // bosta videla iste podatke preko Buffer Poola.
        let mut read_table = Table::open(7001);
        let snapshot_scan = HeapScan::new(bpm.clone(), &mut read_table, tm.clone());

        // 3. Vstavimo podatek preko 'write_table'
        let xid2 = tm.begin();
        let mut tuple = schema.pack(vec![Value::Integer(100)]);
        tuple.header.t_xmin = xid2 as u32;
        write_table.insert_tuple(&tuple);
        tm.commit(xid2);
        tm.flush();

        // 4. Preverimo scan, ki uporablja 'read_table'
        // Snapshot v tem scanu je bil ustvarjen pred commitom xid2, 
        // zato vrstice ne sme videti.
        let count = snapshot_scan.count();
        assert_eq!(count, 0, "Snapshot isolation: scan ne sme videti podatkov, potrjenih po njegovem začetku");
    }

        #[test]
        fn test_mvcc_aborted_transaction() {
            setup(7002);
            let bpm = Arc::new(BufferPoolManager::new(10));
            let tm = Arc::new(TransactionManager::new(300));
            let mut table = Table::open(7002);
            let schema = Schema::new(vec![Column { name: "val".to_string(), data_type: DataType::Integer }]);

            // 1. Vstavimo podatek in nato ABORT-amo transakcijo
            let xid = tm.begin();
            let mut tuple = schema.pack(vec![Value::Integer(666)]);
            tuple.header.t_xmin = xid as u32;
            table.insert_tuple(&tuple);
            tm.abort(xid); // Prekličemo!

            // 2. Scan ne sme videti te vrstice, ker je v CLOG-u status Aborted
            let scan = HeapScan::new(bpm.clone(), &mut table, tm.clone());
            assert_eq!(scan.count(), 0, "Abortirana transakcija mora biti nevidna");
        }

    #[test]
    fn test_mvcc_delete_visibility() { // TODO: Check if test is bad or there is a logical mistake in architecture!
        setup(7003);
        let bpm = Arc::new(BufferPoolManager::new(10));
        let tm = Arc::new(TransactionManager::new(400));
        let mut table = Table::open(7003);
        let schema = Schema::new(vec![Column { name: "val".to_string(), data_type: DataType::Integer }]);

        // 1. Vstavimo originalno vrstico (XID 400) in jo potrdimo
        let xid1 = tm.begin();
        let mut tuple = schema.pack(vec![Value::Integer(1)]);
        tuple.header.t_xmin = xid1 as u32;
        tuple.header.t_xmax = 0; // Ni pobrisana
        table.insert_tuple(&tuple);
        tm.commit(xid1);

        // 2. Simuliramo POBRISANO vrstico (XID 401)
        // Vstavimo novo vrstico, ki ima xmin=400 in xmax=401. 
        // Dokler 401 ni commited, mora biti ta vrstica VIDNA (ker xmax še ni veljaven).
        let xid2 = tm.begin();
        let mut deleted_tuple = schema.pack(vec![Value::Integer(999)]);
        deleted_tuple.header.t_xmin = xid1 as u32; // Ustvarjena v 400
        deleted_tuple.header.t_xmax = xid2 as u32; // Pobrisana v 401
        table.insert_tuple(&deleted_tuple);
        
        // 3. Preverimo vidljivost pred commitom xid2
        {
            let scan_active = HeapScan::new(bpm.clone(), &mut table, tm.clone());
            // Videti moramo obe vrstici:
            // - Originalno (val: 1, xmax: 0)
            // - To, ki se briše (val: 999, xmax: 401), ker 401 še ni Committed
            assert_eq!(scan_active.count(), 2, "Pred commitom xid2 morata biti vidni obe vrstici");
        }

        // 4. Potrdimo transakcijo, ki briše (xid2)
        tm.commit(xid2);
        tm.flush();

        // 5. Preverimo vidljivost po commitu
        let scan_final = HeapScan::new(bpm.clone(), &mut table, tm.clone());
        
        // Sedaj mora biti vrstica z val=999 nevidna, ker je njen xmax (401) potrjen.
        // Ostati mora samo originalna vrstica (val=1).
        let results: Vec<_> = scan_final.collect();
        
        assert_eq!(results.len(), 1, "Po izbrisu mora biti vidna samo še ena vrstica");
        
        // Preverimo, da tista, ki je ostala, NI tista s t_xmax = 401
        let val = i32::from_le_bytes(results[0].data[0..4].try_into().unwrap());
        assert_eq!(val, 1, "Vidna bi morala biti samo originalna vrstica");
        assert_ne!(results[0].header.t_xmax, xid2 as u32, "Vrstica s potrjenim xmax ne sme biti vrnjena");
    }

}