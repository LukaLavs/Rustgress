
use crate::access::tuple::header::{
    HeapTupleHeaderData, Tuple,
};
use crate::access::tuple::header::{
    HeapTupleView, TupleInfoMask,
};
use super::types::{DataType, Value};

#[derive(Debug, Clone)] 
pub struct Column { 
    pub name: String, 
    pub data_type: DataType 
}
#[derive(Debug, Clone)] 
pub struct Schema { 
    pub columns: Vec<Column> 
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self { Schema { columns } }

    pub fn pack(&self, values: Vec<Value>) -> Tuple {
        let mut buffer = Vec::new();
        
        // 1. Priprava bitmape (1 bajt na vsakih 8 stolpcev)
        let bitmap_len = (self.columns.len() + 7) / 8;
        let mut null_bitmap = vec![0u8; bitmap_len];
        let mut has_null = false;

        for (i, value) in values.iter().enumerate() {
            if let Value::Null = value {
                has_null = true;
                // Bit pustimo na 0 (Postgres stil: 0 = NULL, 1 = NOT NULL)
            } else {
                // Nastavimo bit na 1 (NOT NULL)
                null_bitmap[i / 8] |= 1 << (i % 8);
                value.pack(&mut buffer);
            }
        }
        // 2. Izračun t_hoff (Header + Bitmap + Padding)
        let header_size = std::mem::size_of::<HeapTupleHeaderData>();
        let mut hoff = header_size;
        if has_null {
            hoff += null_bitmap.len();
        } else {
            null_bitmap.clear(); // Če ni null-ov, bitmape ne bo v bufferju
        }
        
        // Poravnava na 8 bajtov (MAXALIGN v Postgresu)
        hoff = (hoff + 7) & !7;

        // 3. Nastavitev zastavic
        let mut mask = TupleInfoMask::empty();
        if has_null {
            mask.insert(TupleInfoMask::HEAP_HASNULL);
        }

        // 4. Ustvarjanje glave
        // Opomba: t_infomask2 vsebuje natts v spodnjih 11 bitih
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

        Tuple { header, null_bitmap, data: buffer }
    }

    pub fn unpack(&self, view: &HeapTupleView) -> Vec<Value> {
        self.unpack_raw(view.data(), view.null_bitmap())
    }

    pub fn unpack_from_tuple(&self, tuple: &Tuple) -> Vec<Value> {
        let bitmap = if tuple.null_bitmap.is_empty() { None } else { Some(&tuple.null_bitmap[..]) };
        self.unpack_raw(&tuple.data, bitmap)
    }
}


impl Schema {
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