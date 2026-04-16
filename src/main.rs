
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



fn main() {
    let tm = Arc::new(TransactionManager::new(100));
    let schema = Schema::new(vec![
        Column { name: "id".to_string(), data_type: DataType::Integer },
        Column { name: "is_active".to_string(), data_type: DataType::Boolean },
        Column { name: "tag".to_string(), data_type: DataType::Varchar(100) },
        Column { name: "score".to_string(), data_type: DataType::Integer },
    ]);

    // Cleanup and open
    let _ = std::fs::remove_file("data/1234");
    let mut table = Table::open(1234);

    println!("--- Test 1: Basic Insertion and Retrieval ---");
    let row1 = vec![
        Value::Integer(10),
        Value::Boolean(true),
        Value::Varchar("Standard Tuple".to_string()),
        Value::Integer(100),
    ];
    table.insert_tuple(&schema.pack(row1.clone()));
    
    let page = table.read_page(0);
    if let Some(view) = page.get_tuple_view(1) {
        let unpacked = schema.unpack(&view);
        assert_eq!(unpacked, row1);
        println!("Test 1 Passed");
    }

    println!("--- Test 2: Null Bitmaps (All Nulls and Partial Nulls) ---");
    let row_all_nulls = vec![Value::Null, Value::Null, Value::Null, Value::Null];
    let row_partial_nulls = vec![Value::Integer(5), Value::Null, Value::Varchar("Mixed".to_string()), Value::Null];
    
    table.insert_tuple(&schema.pack(row_all_nulls.clone()));
    table.insert_tuple(&schema.pack(row_partial_nulls.clone()));

    let page = table.read_page(0);
    assert_eq!(schema.unpack(&page.get_tuple_view(2).unwrap()), row_all_nulls);
    assert_eq!(schema.unpack(&page.get_tuple_view(3).unwrap()), row_partial_nulls);
    println!("Test 2 Passed");

    println!("--- Test 3: Large Varchar (Boundary check) ---");
    let long_str = "A".repeat(100);
    let row_long = vec![Value::Integer(99), Value::Boolean(false), Value::Varchar(long_str), Value::Integer(-1)];
    table.insert_tuple(&schema.pack(row_long.clone()));
    
    let page = table.read_page(0);
    assert_eq!(schema.unpack(&page.get_tuple_view(4).unwrap()), row_long);
    println!("Test 3 Passed");

    println!("--- Test 4: Page Overflow (Force new page creation) ---");
    // We insert a lot of large tuples to exceed 8KB
    let large_row = vec![Value::Integer(1), Value::Boolean(true), Value::Varchar("X".repeat(100)), Value::Integer(1)];
    for i in 0..100 {
        table.insert_tuple(&schema.pack(large_row.clone()));
    }
    let pages_count = table.num_pages();
    assert!(pages_count > 1);
    println!("Test 4 Passed: Data spread across {} pages", pages_count);

    println!("--- Test 5: Update and Dead Tuple Visibility ---");
    // Update Tuple 1 on Page 0 (Slot 1)
    let update_row = vec![Value::Integer(10), Value::Boolean(false), Value::Varchar("Updated".to_string()), Value::Integer(200)];
    table.update_tuple(0, 1, &schema.pack(update_row.clone()));

    let page_updated = table.read_page(0);
    // Slot 1 should now be Dead/None
    assert!(page_updated.get_tuple_view(1).is_none());
    // The latest tuple should be at the end of the last page
    let last_page = table.read_page(table.num_pages() - 1);
    
    // Find the last used slot on the last page
    let h = last_page.get_header();
    let header_size = std::mem::size_of::<PageHeaderData>() as u16;
    let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
    let last_slot = (h.pd_lower - header_size) / item_id_size;
    
    let updated_view = last_page.get_tuple_view(last_slot).unwrap();
    assert_eq!(schema.unpack(&updated_view), update_row);
    println!("Test 5 Passed");

    println!("--- Test 6: Zero-length Strings ---");
    let row_empty_str = vec![Value::Integer(0), Value::Boolean(true), Value::Varchar("".to_string()), Value::Integer(0)];
    table.insert_tuple(&schema.pack(row_empty_str.clone()));
    
    let last_page = table.read_page(table.num_pages() - 1);
    // Assuming it's the last slot again
    let h = last_page.get_header();
    let last_slot = (h.pd_lower - header_size) / item_id_size;
    assert_eq!(schema.unpack(&last_page.get_tuple_view(last_slot).unwrap()), row_empty_str);
    println!("Test 6 Passed");

    println!("--- Test 7: Integrity of Transaction IDs and CTIDs ---");
    let row_tx = vec![
        Value::Integer(88),
        Value::Boolean(true),
        Value::Varchar("TX_TEST".to_string()),
        Value::Integer(0),
    ];
    let packed_tx = schema.pack(row_tx);

    // Check if header fields we set manually in Schema::pack (like xmin = 101) persist
    table.insert_tuple(&packed_tx);
    let page = table.read_page(table.num_pages() - 1);
    let h = page.get_header();
    let last_slot = (h.pd_lower - header_size) / item_id_size;

    if let Some(view) = page.get_tuple_view(last_slot) {
        assert_eq!(view.header.t_xmin, 101); // Our hardcoded xmin
        assert_eq!(view.header.t_xmax, 0);   // Should be 0 for new tuples
        println!("Test 7 Passed: Transaction metadata is intact");
    }

    println!("--- Test 8: Large Column Count (Multiple Bitmap Bytes) ---");
    // Create a schema with 20 columns to force the null bitmap to span 3 bytes
    let mut wide_cols = Vec::new();
    for i in 0..20 {
        wide_cols.push(Column { name: format!("c{}", i), data_type: DataType::Boolean });
    }
    let wide_schema = Schema::new(wide_cols);
    let mut wide_values = Vec::new();
    for i in 0..20 {
        // Alternate between value and null
        if i % 2 == 0 { wide_values.push(Value::Boolean(true)); }
        else { wide_values.push(Value::Null); }
    }

    let packed_wide = wide_schema.pack(wide_values.clone());
    table.insert_tuple(&packed_wide);

    let page = table.read_page(table.num_pages() - 1);
    let h = page.get_header();
    let last_slot = (h.pd_lower - header_size) / item_id_size;
    let unpacked_wide = wide_schema.unpack(&page.get_tuple_view(last_slot).unwrap());

    assert_eq!(unpacked_wide, wide_values);
    println!("Test 8 Passed: Multi-byte null bitmap handled correctly");

    println!("--- Test 9: Max Length Varchar (u16::MAX boundary) ---");
    // Note: BLCKSZ is 8192, so we can't actually fit 64KB, but we test a large string
    let max_fit = 7000; 
    let row_huge = vec![
        Value::Integer(1),
        Value::Boolean(true),
        Value::Varchar("Z".repeat(max_fit)),
        Value::Integer(2),
    ];
    table.insert_tuple(&schema.pack(row_huge.clone()));

    let last_page = table.read_page(table.num_pages() - 1);
    let h = last_page.get_header();
    let last_slot = (h.pd_lower - header_size) / item_id_size;
    assert_eq!(schema.unpack(&last_page.get_tuple_view(last_slot).unwrap()), row_huge);
    println!("Test 9 Passed: Large Varlena (7000 bytes) stored and retrieved");

    println!("--- Test 10: Delete-only Logic ---");
    // Test if deleting a tuple actually makes it invisible but preserves the slot space
    let before_delete_h = table.read_page(0).get_header();
    table.delete_tuple(0, 2); // Delete the 2nd slot on page 0
    let after_delete_page = table.read_page(0);
    let after_delete_h = after_delete_page.get_header();

    // Metadata check
    assert!(after_delete_page.get_tuple_view(2).is_none());
    let lower_before = before_delete_h.pd_lower;
    let lower_after = after_delete_h.pd_lower;

    assert_eq!(lower_before, lower_after);
    println!("Test 10 Passed: Slot marked DEAD and invisible");

    println!("--- Test 11: Boolean Byte Interpretation ---");
    // Ensure boolean 0/1 doesn't conflict with neighboring integers
    let row_bools = vec![
        Value::Integer(16777216), // 0x01000000 in LE
        Value::Boolean(false),    // 0x00
        Value::Varchar("bool".to_string()),
        Value::Integer(1),
    ];
    table.insert_tuple(&schema.pack(row_bools.clone()));
    let page = table.read_page(table.num_pages() - 1);
    let h = page.get_header();
    let last_slot = (h.pd_lower - header_size) / item_id_size;
    assert_eq!(schema.unpack(&page.get_tuple_view(last_slot).unwrap()), row_bools);
    println!("Test 11 Passed: Boolean alignment and value check successful");

    println!("---- Test 12: Catalogs ---");
    let _ = std::fs::remove_dir_all("data");
    bootstrap_system_catalogs();
    test_catalogs();

    println!("\n--- Test 13: BufferPoolManager & HeapScan (The Real Deal) ---");
    let bpm = Arc::new(BufferPoolManager::new(10));
    let tm = Arc::new(TransactionManager::new(100)); // XID se začne pri 100

    let test_oid = 5000;
    let mut test_table = Table::open(test_oid);
    let test_schema = Schema::new(vec![
        Column { name: "val".to_string(), data_type: DataType::Integer },
    ]);

    // 1. ZAČNI TRANSAKCIJO
    let xid = tm.begin();

    // 2. VSTAVI PODATKE (Poskrbi, da insert_tuple uporabi ta xid!)
    for i in 1..=200 {
        let mut tuple = test_schema.pack(vec![Value::Integer(i)]);
        tuple.header.t_xmin = xid as u32; // <--- NUJNO: Vrstica mora imeti pravi XID
        test_table.insert_tuple(&tuple);
    }

    // 3. POTRDI TRANSAKCIJO
    tm.commit(xid);
    tm.flush(); // Zapiši v CLOG na disk

    // 4. USTVARI SCAN (Zdaj, ko je XID commited in ni več v active_xids)
    let scan = HeapScan::new(bpm.clone(), &mut test_table, tm.clone());

    let mut sum = 0;
    let mut count = 0;
    for tuple in scan {
        let val = i32::from_le_bytes(tuple.data[0..4].try_into().unwrap());
        sum += val;
        count += 1;
    }

    println!("Scan zaključen. Prešteto {} vrstic, vsota: {}", count, sum);
    assert_eq!(count, 200);
    assert_eq!(sum, (1..=200).sum());
    println!("Test 13 Passed: HeapScan preko BPM deluje!");

    println!("\n--- Test 14: Buffer Eviction Stress Test ---");
    let tiny_bpm = Arc::new(BufferPoolManager::new(2));
    let scan_tiny = HeapScan::new(tiny_bpm.clone(), &mut test_table, tm.clone());
    
    let count_tiny = scan_tiny.count();
    assert_eq!(count_tiny, 200);
    println!("Test 14 Passed: Scan uspešen kljub majhnemu bufferju (Eviction deluje).");

println!("\n--- Test 15: Cross-Table Buffer Integrity (MVCC Version) ---");
    
    // 1. Priprava nove tabele
    let mut other_table = Table::open(6000);
    
    // 2. Začnemo transakcijo za vstavljanje v drugo tabelo
    let xid_other = tm.begin();
    
    // 3. Pripravimo tuple in mu ročno nastavimo t_xmin (dokler nimaš Transactional Table API-ja)
    let mut tuple_999_raw = test_schema.pack(vec![Value::Integer(999)]);
    tuple_999_raw.header.t_xmin = xid_other as u32; // <--- Nastavimo XID transakcije
    
    other_table.insert_tuple(&tuple_999_raw);
    
    // 4. POTRDIMO transakcijo, da postane vidna za prihodnje snapshote
    tm.commit(xid_other);
    tm.flush(); // Shranimo stanje v CLOG na disk

    {
        // 5. Ustvarimo scan-a. 
        // HeapScan::new bo vzel nov Snapshot, ki bo videl Commited xid_other.
        let mut scan_a = HeapScan::new(bpm.clone(), &mut test_table, tm.clone());
        let mut scan_b = HeapScan::new(bpm.clone(), &mut other_table, tm.clone());

        // Zdaj .next() ne bi smel več vrniti None
        let tuple_a = scan_a.next().expect("Tabela A (5000) bi morala vrniti tuple (1)");
        let tuple_b = scan_b.next().expect("Tabela B (6000) bi morala vrniti tuple (999)");

        let val_a_raw = i32::from_le_bytes(tuple_a.data[0..4].try_into().unwrap());
        let val_b_raw = i32::from_le_bytes(tuple_b.data[0..4].try_into().unwrap());

        println!("Tabela A (5000) prvi element: {:?}", val_a_raw);
        println!("Tabela B (6000) prvi element: {:?}", val_b_raw);

        assert_eq!(val_a_raw, 1, "Vrednost v tabeli A bi morala biti 1");
        assert_eq!(val_b_raw, 999, "Vrednost v tabeli B bi morala biti 999");
    }

    println!("Test 15 Passed: Buffer distinguishes OIDs and respects MVCC.");

    println!("Test 16:");
    run_complex_scan_test();
    println!("test 16 passed.");

    println!("All storage tests passed successfully.");
}


fn test_catalogs() {
    println!("\n--- TEST: System Catalogs Integrity ---");

    // 1. Verify rg_class
    let mut class_table = Table::open(RGClass::get_oid());
    let class_schema = RGClass::get_schema();

    println!("Checking rg_class.db...");
    let page_class = class_table.read_page(0);
    
    // Slot 1: rg_class definition itself
    if let Some(view) = page_class.get_tuple_view(1) {
        let unpacked = class_schema.unpack(&view);
        println!("Catalog entry 1: {:?}", unpacked[1]); 
        assert_eq!(unpacked[1], Value::Varchar("rg_class".to_string()));
    }

    // Slot 2: rg_attributes definition
    if let Some(view) = page_class.get_tuple_view(2) {
        let unpacked = class_schema.unpack(&view);
        println!("Catalog entry 2: {:?}", unpacked[1]);
        assert_eq!(unpacked[1], Value::Varchar("rg_attributes".to_string()));
    }

    // 2. Verify rg_attributes
    let mut attr_table = Table::open(RGAttribute::get_oid());
    let attr_schema = RGAttribute::get_schema();

    println!("\nChecking rg_attributes.db...");
    let page_attr = attr_table.read_page(0);
    
    // We expect at least 10 entries (5 for rg_class, 5 for rg_attributes)
    for i in 1..=10 {
        if let Some(view) = page_attr.get_tuple_view(i) {
            let unpacked = attr_schema.unpack(&view);
            // Uporabimo {:?} za vse, ker Value implementira Debug
    println!("Slot {:<2} | Column: {:<15?} | Table OID: {:?}", i, unpacked[1], unpacked[0]);
        } else {
            println!("Slot {} is empty or missing!", i);
        }
    }

    println!("--- System Catalog Test Passed ---");
}




pub fn run_complex_scan_test() {
    println!("==================================================");
    println!("ZAČETEK TESTA: Scenarij z dvema tabelama in BPM");
    println!("==================================================");

    // 1. Priprava Buffer Poola (majhen, da vidimo, če se kaj dogaja)
    let bpm = Arc::new(BufferPoolManager::new(5));
    println!("[BPM] Inicializiran s 5 okvirji.");

    // 2. Definiranje sheme
    let schema = Schema::new(vec![
        Column { name: "id".to_string(), data_type: DataType::Integer },
        Column { name: "ime".to_string(), data_type: DataType::Varchar(50) },
    ]);

    // 3. Ustvarjanje prve tabele (OID 101) - UPORABNIKI
    let oid_users = 101;
    let _ = std::fs::remove_file(format!("data/{}", oid_users));
    let mut table_users = Table::open(oid_users);
    
    println!("\n[Sistem] Vstavljam 5 vrstic v tabelo UPORABNIKI (OID 101)...");
    for i in 1..=5 {
        let row = vec![Value::Integer(i), Value::Varchar(format!("Uporabnik-{}", i))];
        table_users.insert_tuple(&schema.pack(row));
    }

    // 4. Ustvarjanje druge tabele (OID 202) - IZDELKI
    let oid_products = 202;
    let _ = std::fs::remove_file(format!("data/{}", oid_products));
    let mut table_products = Table::open(oid_products);

    println!("[Sistem] Vstavljam 5 vrstic v tabelo IZDELKI (OID 202)...");
    for i in 1..=5 {
        let row = vec![Value::Integer(i + 100), Value::Varchar(format!("Izdelek-{}", i))];
        table_products.insert_tuple(&schema.pack(row));
    }

    // 5. Branje prve tabele s HeapScanom
    println!("\n--- SKENIRANJE TABELE UPORABNIKI ---");
    let tm = Arc::new(TransactionManager::new(100));
    let scan_users = HeapScan::new(bpm.clone(), &mut table_users, tm.clone());
    for (idx, tuple) in scan_users.enumerate() {
        let data = schema.unpack_from_tuple(&tuple); // Predpostavljam, da si dodal to metodo
        println!("  Row {}: {:?}", idx + 1, data);
    }

    // 6. Branje druge tabele s HeapScanom
    println!("\n--- SKENIRANJE TABELE IZDELKI ---");
    let scan_products = HeapScan::new(bpm.clone(), &mut table_products, tm.clone());
    for (idx, tuple) in scan_products.enumerate() {
        let data = schema.unpack_from_tuple(&tuple);
        println!("  Row {}: {:?}", idx + 1, data);
    }

    // 7. Preverjanje stanja Bufferja
    // Ker smo prebrali obe tabeli, bi morali biti v page_table oznake za obe tabeli
    println!("\n[BPM] Trenutno stanje v Buffer Poolu:");
    // Tukaj bi lahko dodal debug izpis v BPM, če ga imaš, npr:
    // bpm.print_debug_status();

    println!("==================================================");
    println!("TEST USPEŠNO ZAKLJUČEN");
    println!("==================================================");
}