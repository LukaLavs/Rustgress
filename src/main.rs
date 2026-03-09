use std::ptr;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use bitfield::bitfield;

pub const BLCKSZ: usize = 8192; // page size in bytes

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct PageHeaderData { // 16 bytes
    pub pd_checksum: u16, // checksum
    pub pd_flags: u16, // flag bits, see below
    pub pd_lower: LocationIndex, // offset to start of free space
    pub pd_upper: LocationIndex, // offset to end of free space
    pub pd_special: LocationIndex, // offset to start of special space
    pub pd_pagesize_version: PageSizeVersion, // page size and version info
    pub pd_prune_xid: TransactionId, // oldest prunable transaction
}

pub const PD_HAS_FREE_LINES: u16 = 0x0001; // are there any unused line pointers?
pub const PD_PAGE_FULL: u16 = 0x0002; // not enough free space for new tuple?
pub const PD_ALL_VISIBLE: u16 = 0x0004; // all tuples on page are visible to everyone?
pub const PD_VALID_FLAG_BITS: u16 = 0x0007; // all valid flag bits

pub type LocationIndex = u16;
pub type TransactionId = u32;
pub type PageSizeVersion = u16;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct HeapTupleHeaderData {
    pub t_xmin: TransactionId, // id of inserting transaction
    pub t_xmax: TransactionId, // id of deleting transaction, 0 if alive
    pub t_ctid_page: u32,
    pub t_ctid_slot: u16,
    pub t_infomask2: u16,
    pub t_infomask: u16,
    pub t_hoff: u8,
    pub t_null_bitmap: u16,      
    pub t_padding: [u8; 3],
}

pub struct Tuple {
    pub header: HeapTupleHeaderData,
    pub data: HeapTupleData,
}

type HeapTupleData = Vec<u8>;

impl Tuple {
    /// tuple serialization: \[header|data|padding\]
    pub fn serialize(&self) -> HeapTupleData {
        let mut buffer: HeapTupleData = Vec::new();
        let header_slice = unsafe {
            std::slice::from_raw_parts(
                &self.header as *const HeapTupleHeaderData as *const u8,
                std::mem::size_of::<HeapTupleHeaderData>()
            )
        };
        buffer.extend_from_slice(header_slice);
        buffer.extend_from_slice(&self.data);
        while buffer.len() % 8 != 0 { buffer.push(0); } // align bytes
        buffer // byte array ready to be stored on page
    }
}

bitfield! {
    /// line pointer (4 bytes), consisting of:
    /// - (lp_off) offset to tuple (15 bits)
    /// - (lp_flags) state of line pointer (2 bits)
    /// - (lp_len) byte length of tuple (15 bits)
    pub struct ItemIdData(u32);
    impl Debug;
    pub lp_off, set_lp_off: 14, 0;
    pub lp_flags, set_lp_flags: 16, 15; // state of line pointer, see below
    pub lp_len, set_lp_len: 31, 17;
}

/// lp_flags has these possible states.  An UNUSED line pointer is available
/// for immediate re-use, the other states are not.
#[repr(u8)]
pub enum ItemIdFlags {
    Unused = 0,
    Normal = 1,
    Redirect = 2,
    Dead = 3,
}


/// raw page data, consisting of:
/// 1. Page header (16 bytes)
/// 2. Line pointers (each 4 bytes)
/// 3. Tuple data (variable length)
/// 4. Optional special space (2 bytes)
#[repr(align(8))]
pub struct Page {
    pub data: [u8; BLCKSZ],
}


impl Page {
    pub fn new() -> Self {
        let mut page = Page { data: [0u8; BLCKSZ] };
        page.init();
        page
    }

    fn init(&mut self) {
        let header_size = std::mem::size_of::<PageHeaderData>() as u16;
        let h = self.header_mut();
        h.pd_lower = header_size;
        h.pd_upper = BLCKSZ as u16;
        h.pd_special = BLCKSZ as u16;
    }

    pub fn header(&self) -> &PageHeaderData {
        unsafe { &*(self.data.as_ptr() as *const PageHeaderData) }
    }

    pub fn header_mut(&mut self) -> &mut PageHeaderData {
        unsafe { &mut *(self.data.as_mut_ptr() as *mut PageHeaderData) }
    }

    pub fn get_free_space(&self) -> usize {
        let h = self.header();
        (h.pd_upper.saturating_sub(h.pd_lower)) as usize
    }

    pub fn add_tuple(&mut self, tuple: &Tuple) -> Option<OffsetNumber> {
        let serialized: HeapTupleData = tuple.serialize();
        let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
        let needed_space = serialized.len() as u16 + item_id_size;

        if self.get_free_space() < needed_space as usize {
            return None;
        }

        let lp_off: u16;
        let lower_ptr_pos: u16;
        {
            let h = self.header_mut();
            h.pd_upper -= serialized.len() as u16;
            lp_off = h.pd_upper;
            lower_ptr_pos = h.pd_lower;
            h.pd_lower += item_id_size;
        }

        let mut item_id = ItemIdData(0);
        item_id.set_lp_off(lp_off as u32);
        item_id.set_lp_flags(ItemIdFlags::Normal as u32);
        item_id.set_lp_len(serialized.len() as u32);

        unsafe {
            let item_id_ptr = self.data.as_mut_ptr().add(lower_ptr_pos as usize) as *mut ItemIdData;
            ptr::write_unaligned(item_id_ptr, item_id);
        }

        self.data[lp_off as usize..(lp_off as usize + serialized.len())].copy_from_slice(&serialized);
        let header_size = std::mem::size_of::<PageHeaderData>() as u16;
        Some((lower_ptr_pos - header_size) / item_id_size + 1)
    }

    pub fn get_tuple_data(&self, slot_num: OffsetNumber) -> Option<(HeapTupleHeaderData, &[u8])> {
        if slot_num == 0 { return None; }
        let header_size = std::mem::size_of::<PageHeaderData>() as u16;
        let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
        let offset_to_id = header_size + (slot_num - 1) * item_id_size;
        
        if offset_to_id + item_id_size > self.header().pd_lower { return None; }

        let item_id = unsafe {
            let ptr = self.data.as_ptr().add(offset_to_id as usize) as *const ItemIdData;
            ptr::read_unaligned(ptr)
        };

        // lp_flags: 1 je NORMAL, 2 je DEAD
        if item_id.lp_flags() != ItemIdFlags::Normal as u32 { return None; }

        let start = item_id.lp_off() as usize;
        let end = (item_id.lp_off() + item_id.lp_len()) as usize;
        let raw_tuple = &self.data[start..end];
        
        let header = unsafe {
            let ptr = raw_tuple.as_ptr() as *const HeapTupleHeaderData;
            ptr::read_unaligned(ptr)
        };

        let data_start = header.t_hoff as usize;
        Some((header, &raw_tuple[data_start..]))
    }
}

pub type OffsetNumber = u16;

pub struct Table { file: File }
impl Table {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        let file = OpenOptions::new().read(true).write(true).create(true).open(path).expect("File error");
        Table { file }
    }
    pub fn read_page(&mut self, page_id: u32) -> Page {
        let mut page = Page::new();
        self.file.seek(SeekFrom::Start(page_id as u64 * BLCKSZ as u64)).unwrap();
        let _ = self.file.read_exact(&mut page.data);
        page
    }
    pub fn write_page(&mut self, page_id: u32, page: &Page) {
        self.file.seek(SeekFrom::Start(page_id as u64 * BLCKSZ as u64)).unwrap();
        self.file.write_all(&page.data).unwrap();
    }
    pub fn num_pages(&self) -> u32 {
        self.file.metadata().unwrap().len() as u32 / BLCKSZ as u32
    }
    pub fn insert_tuple(&mut self, tuple: &Tuple) {
        let mut last_page_id = self.num_pages().saturating_sub(1);
        let mut page = if self.num_pages() == 0 { last_page_id = 0; Page::new() } else { self.read_page(last_page_id) };
        if page.add_tuple(tuple).is_some() {
            self.write_page(last_page_id, &page);
        } else {
            let mut new_page = Page::new();
            new_page.add_tuple(tuple);
            self.write_page(last_page_id + 1, &new_page);
        }
    }
    pub fn delete_tuple(&mut self, page_id: u32, slot_num: OffsetNumber) {
        let mut page = self.read_page(page_id);
        
        let header_size = std::mem::size_of::<PageHeaderData>() as u16;
        let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
        let offset_to_id = header_size + (slot_num - 1) * item_id_size;

        if offset_to_id + item_id_size <= page.header().pd_lower {
            unsafe {
                let item_id_ptr = page.data.as_mut_ptr().add(offset_to_id as usize) as *mut ItemIdData;
                let mut item_id = ptr::read_unaligned(item_id_ptr);

                item_id.set_lp_flags(ItemIdFlags::Dead as u32);
                
                ptr::write_unaligned(item_id_ptr, item_id);
            }
            self.write_page(page_id, &page);
        }
    }
    pub fn update_tuple(&mut self, page_id: u32, slot_num: OffsetNumber, new_tuple: &Tuple) {
        self.delete_tuple(page_id, slot_num); // delete old tuple (mark as dead)

        self.insert_tuple(new_tuple); // insert new tuple
    }
}

#[derive(Debug, Clone, PartialEq)] 
pub enum DataType { 
    Integer, 
    Varchar(u16),
    Boolean,
    Timestamp
}
#[derive(Debug, Clone)] 
pub struct Column { 
    pub name: String, 
    pub data_type: DataType 
}
#[derive(Debug, Clone)] 
pub struct Schema { 
    pub columns: Vec<Column> 
}

#[derive(Debug, Clone, PartialEq)] 
pub enum Value { 
    Integer(i32), 
    Varchar(String),
    Boolean(bool),
    Timestamp(i64),
    Null
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self { Schema { columns } }
    pub fn pack(&self, values: Vec<Value>) -> Tuple {
        let mut buffer = Vec::new();
        let mut null_bitmap: u16 = 0; // 0 represents NULL, 1 represents NOT NULL for each column

        for (i, value) in values.iter().enumerate() {
            if *value != Value::Null {
                null_bitmap |= 1 << i; 
                match (value, &self.columns[i].data_type) {
                    (Value::Integer(v), DataType::Integer) => buffer.extend_from_slice(&v.to_le_bytes()),
                    (Value::Boolean(b), DataType::Boolean) => buffer.push(if *b { 1 } else { 0 }),
                    (Value::Timestamp(t), DataType::Timestamp) => buffer.extend_from_slice(&t.to_le_bytes()),
                    (Value::Varchar(s), DataType::Varchar(_)) => {
                        buffer.extend_from_slice(&(s.len() as u16).to_le_bytes());
                        buffer.extend_from_slice(s.as_bytes());
                    }
                    _ => panic!("Type mismatch"),
                }
            }
        }

        let header = HeapTupleHeaderData {
            t_xmin: 101,
            t_xmax: 0,
            t_ctid_page: 0,
            t_ctid_slot: 0,
            t_infomask2: self.columns.len() as u16,
            t_infomask: 0,
            t_hoff: std::mem::size_of::<HeapTupleHeaderData>() as u8,
            t_null_bitmap: null_bitmap,
            t_padding: [0; 3],
        };

        Tuple { header, data: buffer }
    }

    pub fn unpack(&self, header: &HeapTupleHeaderData, raw_data: &[u8]) -> Vec<Value> {
        let mut values = Vec::new();
        let mut cursor = 0;

        for (i, col) in self.columns.iter().enumerate() {
            let is_not_null = (header.t_null_bitmap & (1 << i)) != 0;

            if !is_not_null {
                values.push(Value::Null);
                continue;
            }

            match col.data_type {
                DataType::Integer => {
                    let val = i32::from_le_bytes(raw_data[cursor..cursor+4].try_into().unwrap());
                    values.push(Value::Integer(val));
                    cursor += 4;
                }
                DataType::Boolean => {
                    values.push(Value::Boolean(raw_data[cursor] == 1));
                    cursor += 1;
                }
                DataType::Timestamp => {
                    let val = i64::from_le_bytes(raw_data[cursor..cursor+8].try_into().unwrap());
                    values.push(Value::Timestamp(val));
                    cursor += 8;
                }
                DataType::Varchar(_) => {
                    let len = u16::from_le_bytes(raw_data[cursor..cursor+2].try_into().unwrap()) as usize;
                    cursor += 2;
                    let s = std::str::from_utf8(&raw_data[cursor..cursor+len]).unwrap();
                    values.push(Value::Varchar(s.to_string()));
                    cursor += len;
                }
            }
        }
        values
    }
}

fn main() {
    // 1. Priprava sheme (id, aktivnost, časovni žig, opomba)
    let schema = Schema {
        columns: vec![
            Column { name: "id".to_string(), data_type: DataType::Integer },
            Column { name: "is_active".to_string(), data_type: DataType::Boolean },
            Column { name: "updated_at".to_string(), data_type: DataType::Timestamp },
            Column { name: "tag".to_string(), data_type: DataType::Varchar(20) },
        ]
    };

    let mut table = Table::open("data.db");
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;

    // 2. Direkten vnos različnih testnih primerov
    println!("--- Vstavljanje podatkov ---");
    
    // Poln zapis
    table.insert_tuple(&schema.pack(vec![
        Value::Integer(101),
        Value::Boolean(true),
        Value::Timestamp(now),
        Value::Varchar("PRVI".to_string()),
    ]));

    // Zapis z NULL vrednostjo (opomba manjka)
    table.insert_tuple(&schema.pack(vec![
        Value::Integer(102),
        Value::Boolean(false),
        Value::Timestamp(now + 1000),
        Value::Null, 
    ]));

    // 3. Izpis vsebine tabele
    println!("--- Branje tabele ---");
    let total_pages = table.num_pages();
    
    for p_id in 0..total_pages {
        let page = table.read_page(p_id);
        
        // Dinamično ugotovimo število slotov na strani
        let header_size = std::mem::size_of::<PageHeaderData>() as u16;
        let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
        let num_slots = page.header().pd_lower.saturating_sub(header_size) / item_id_size;

        for slot in 1..=num_slots {
            if let Some((header, raw_data)) = page.get_tuple_data(slot) {
                let row = schema.unpack(&header, raw_data);
                
                // Lepši formatiran izpis
                println!("Stran {}, Slot {}: ", p_id, slot);
                for (col, val) in schema.columns.iter().zip(row.iter()) {
                    println!("  | {:<10}: {:?}", col.name, val);
                }
                println!("  -----------------------");
            }
        }
    }
    
    println!("Skupno število strani na disku: {}", total_pages);
}