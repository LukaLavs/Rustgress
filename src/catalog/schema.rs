
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

                match (value, &self.columns[i].data_type) {
                    (Value::Integer(v), DataType::Integer) => buffer.extend_from_slice(&v.to_le_bytes()),
                    (Value::Boolean(b), DataType::Boolean) => buffer.push(if *b { 1 } else { 0 }),
                    (Value::Timestamp(t), DataType::Timestamp) => buffer.extend_from_slice(&t.to_le_bytes()),
                    (Value::Varchar(s), DataType::Varchar(_)) => {
                        buffer.extend_from_slice(&(s.len() as u16).to_le_bytes());
                        buffer.extend_from_slice(s.as_bytes());
                    }
                    (Value::Float(f), DataType::Float) => {
                        buffer.extend_from_slice(&f.to_le_bytes());
                    }
                    (Value::Double(d), DataType::Double) => {
                        // Hardcore performance: Double bi moral biti poravnan na 8 bajtov
                        // Za zdaj ga dodamo direktno, a v prihodnje lahko tukaj dodava padding
                        buffer.extend_from_slice(&d.to_le_bytes());
                    }
                    (Value::Numeric(s), DataType::Numeric(_, _)) => {
                        buffer.extend_from_slice(&(s.len() as u16).to_le_bytes());
                        buffer.extend_from_slice(s.as_bytes());
                    }
                    _ => panic!("Type mismatch for column {}", self.columns[i].name),
                }
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
            t_xmin: 101,
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
        let mut values = Vec::new();
        let mut cursor = 0;
        let _header = view.header();
        let raw_data = view.data();
        
        // Pridobimo bitmapo iz view-a, če obstaja
        let bitmap = view.null_bitmap();

        for (i, col) in self.columns.iter().enumerate() {
            // Preverimo NULL stanje
            let is_not_null = if let Some(map) = bitmap {
                (map[i / 8] & (1 << (i % 8))) != 0
            } else {
                true // Če bitmape ni, so vsi NOT NULL
            };

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
                    if cursor + 2 > raw_data.len() { panic!("Cursor out of bounds for VARCHAR len"); }
                    let len = u16::from_le_bytes(raw_data[cursor..cursor+2].try_into().unwrap()) as usize;
                    cursor += 2;
                    if cursor + len > raw_data.len() {
                        panic!("VARLENA ERROR: Tuple data too short! Want {}, have {}. Cursor: {}, hoff: {}", 
                            len, raw_data.len() - cursor, cursor, view.header.t_hoff);
                    }
                    let s = std::str::from_utf8(&raw_data[cursor..cursor+len]).unwrap();
                    values.push(Value::Varchar(s.to_string()));
                    cursor += len;
                }
                DataType::Float => {
                    let val = f32::from_le_bytes(raw_data[cursor..cursor+4].try_into().unwrap());
                    values.push(Value::Float(val));
                    cursor += 4;
                }
                DataType::Double => {
                    let val = f64::from_le_bytes(raw_data[cursor..cursor+8].try_into().unwrap());
                    values.push(Value::Double(val));
                    cursor += 8;
                }
                DataType::Numeric(_, _) => {
                    // same as Varlena
                    if cursor + 2 > raw_data.len() { panic!("Cursor out of bounds for Numeric len"); }
                    let len = u16::from_le_bytes(raw_data[cursor..cursor+2].try_into().unwrap()) as usize;
                    cursor += 2;
                    
                    if cursor + len > raw_data.len() {
                        panic!("NUMERIC VARLENA ERROR: Tuple data too short!");
                    }
                    
                    let s = std::str::from_utf8(&raw_data[cursor..cursor+len]).expect("Invalid Numeric UTF-8");
                    values.push(Value::Numeric(s.to_string()));
                    cursor += len;
                }
            }
        }
        values
    }
}

impl Schema {
    pub fn unpack_from_tuple(&self, tuple: &Tuple) -> Vec<Value> {
        let mut values = Vec::new();
        let mut cursor = 0;

        for (i, col) in self.columns.iter().enumerate() {
            let is_null = if !tuple.null_bitmap.is_empty() {
                (tuple.null_bitmap[i / 8] & (1 << (i % 8))) == 0
            } else {
                false
            };

            if is_null {
                values.push(Value::Null);
                continue;
            }
            
            match col.data_type {
                DataType::Integer => {
                    let val = i32::from_le_bytes(tuple.data[cursor..cursor+4].try_into().unwrap());
                    values.push(Value::Integer(val));
                    cursor += 4;
                }
                DataType::Boolean => {
                    values.push(Value::Boolean(tuple.data[cursor] == 1));
                    cursor += 1;
                }
                DataType::Timestamp => {
                    let val = i64::from_le_bytes(tuple.data[cursor..cursor+8].try_into().unwrap());
                    values.push(Value::Timestamp(val));
                    cursor += 8;
                }
                DataType::Varchar(_) | DataType::Numeric(_, _) => {
                    let len = u16::from_le_bytes(tuple.data[cursor..cursor+2].try_into().unwrap()) as usize;
                    cursor += 2;
                    let s = std::str::from_utf8(&tuple.data[cursor..cursor+len]).unwrap();
                    
                    if let DataType::Varchar(_) = col.data_type {
                        values.push(Value::Varchar(s.to_string()));
                    } else {
                        values.push(Value::Numeric(s.to_string()));
                    }
                    cursor += len;
                }
                DataType::Float => {
                    let val = f32::from_le_bytes(tuple.data[cursor..cursor+4].try_into().unwrap());
                    values.push(Value::Float(val));
                    cursor += 4;
                }
                DataType::Double => {
                    let val = f64::from_le_bytes(tuple.data[cursor..cursor+8].try_into().unwrap());
                    values.push(Value::Double(val));
                    cursor += 8;
                }
            }
        }
        values
    }
}