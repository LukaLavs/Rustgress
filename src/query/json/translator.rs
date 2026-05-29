use serde_json::{Value as JsonValue, Map, json};
use crate::access::tuple::desc::TupleDescriptor;
use crate::utils::adt::datatype::{Value};

pub struct WebTranslator;

impl WebTranslator {
    pub fn to_web_json(schema: &TupleDescriptor, rows: &[Vec<Value>]) -> String {
        let mut json_rows = Vec::new();

        for row in rows {
            let mut row_map = Map::new();
            for (i, col) in schema.columns.iter().enumerate() {
                if let Some(val) = row.get(i) {
                    let json_val = val.as_json();
                    row_map.insert(col.name.clone(), json_val);
                }
            }
            json_rows.push(JsonValue::Object(row_map));
        }
        let final_response = json!({
            "status": "success",
            "row_count": json_rows.len(),
            "columns": schema.columns.iter().map(|c| c.name.clone()).collect::<Vec<String>>(),
            "data": json_rows
        });
        serde_json::to_string_pretty(&final_response).unwrap_or_else(|_| "{}".to_string())
    }
}