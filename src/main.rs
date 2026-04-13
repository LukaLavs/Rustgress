
use rustgress::storage::page::header::PageHeaderData;
use rustgress::access::tuple::header::{
    ItemIdData,
};

use rustgress::catalog::schema::{Schema, Column, DataType, Value};
use rustgress::storage::disk::manager::Table;
// use std::time::{SystemTime, UNIX_EPOCH};


fn main() {
    let schema = Schema::new(vec![
        Column { name: "id".to_string(), data_type: DataType::Integer },
        Column { name: "is_active".to_string(), data_type: DataType::Boolean },
        Column { name: "tag".to_string(), data_type: DataType::Varchar(100) },
        Column { name: "score".to_string(), data_type: DataType::Integer },
    ]);

    // Cleanup and open
    let _ = std::fs::remove_file("data.db");
    let mut table = Table::open("data.db");

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

    println!("All storage tests passed successfully.");
}