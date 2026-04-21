use crate::access::tuple::header::{TupleInfoMask, HeapTupleHeaderData};
use crate::access::tuple::tuple::{HeapTupleView, HeapTuple};
use crate::catalog::types::{DataType, Value};

#[derive(Debug, Clone)] 
pub struct Column { 
    pub name: String, 
    pub data_type: DataType 
}
#[derive(Debug, Clone)] 
pub struct TupleDescriptor { 
    pub columns: Vec<Column> 
}

impl TupleDescriptor {
    pub fn new(columns: Vec<Column>) -> Self { TupleDescriptor { columns } }

    pub fn pack(&self, values: Vec<Value>) -> HeapTuple {
        let mut buffer = Vec::new();
        
        let bitmap_len = (self.columns.len() + 7) / 8; // alignment!
        let mut null_bitmap = vec![0u8; bitmap_len];
        let mut has_null = false;

        for (i, value) in values.iter().enumerate() {
            if let Value::Null = value {
                has_null = true;
                // leave bit as 0 for NULL
            } else {
                null_bitmap[i / 8] |= 1 << (i % 8); // set bit to 1 for NOT NULL
                value.pack(&mut buffer);
            }
        }
        let header_size = std::mem::size_of::<HeapTupleHeaderData>();
        let mut hoff = header_size;
        if has_null {
            hoff += null_bitmap.len();
        } else {
            null_bitmap.clear(); // no null bitmap if there are no nulls
        }
        
        hoff = (hoff + 7) & !7; // 8 byte alignment (MAXALIGN)

        let mut mask = TupleInfoMask::empty();
        if has_null {
            mask.insert(TupleInfoMask::HEAP_HASNULL);
        }
        let header = HeapTupleHeaderData {
            t_xmin: 101, // TODO: hardcoded, should be set by transaction manager
            t_xmax: 0,
            t_ctid_page: 0,
            t_ctid_slot: 0,
            t_infomask2: self.columns.len() as u16 & 0x07FF, 
            t_infomask: mask.bits(),
            t_padding: 0,
            t_hoff: hoff as u8,
        };

        HeapTuple { header, null_bitmap, data: buffer }
    }

    pub fn unpack(&self, view: &HeapTupleView) -> Vec<Value> {
        self.unpack_raw(view.data(), view.null_bitmap())
    }

    pub fn unpack_from_tuple(&self, tuple: &HeapTuple) -> Vec<Value> {
        let bitmap = if tuple.null_bitmap.is_empty() { None } else { Some(&tuple.null_bitmap[..]) };
        self.unpack_raw(&tuple.data, bitmap)
    }
}


impl TupleDescriptor {
    pub fn unpack_raw(&self, raw_data: &[u8], bitmap: Option<&[u8]>) -> Vec<Value> {
        let mut values = Vec::new();
        let mut cursor = 0;
        for (i, col) in self.columns.iter().enumerate() {
            let is_not_null = bitmap.map_or(true, |b| (b[i/8] & (1 << (i%8))) != 0);
            if is_not_null {
                values.push(col.data_type.unpack(raw_data, &mut cursor));
            } else {
                values.push(Value::Null);
            }
        }
        values
    }
}