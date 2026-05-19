use serde_json::{Value as JsonValue, Map, json};
use crate::access::tuple::desc::TupleDescriptor;
// Predvidevam, da imaš nek tak enum za tvoje interne vrednosti (prilagodi dejanskemu imenu):
use crate::catalog::types::Value; 

pub struct WebTranslator;

impl WebTranslator {
    /// Pretvori posamezno interno vrednost baze v Serde JSON vrednost
    fn value_to_json(val: &Value) -> JsonValue {
        match val {
            Value::Integer(i) => json!(i),
            Value::Varchar(s) => json!(s),
            Value::Float(f) => json!(f),
            Value::Boolean(b) => json!(b),
            // Dodaj morebitne ostale tipe, ki jih podpira tvoj DataType enum
            _ => JsonValue::Null,
        } // TODO: Types should implement to json or something
    }

    /// Glavna funkcija, ki pretvori celoten rezultat poizvedbe v JSON format primeren za Web API
    pub fn to_web_json(schema: &TupleDescriptor, rows: &[Vec<Value>]) -> String {
        let mut json_rows = Vec::new();

        for row in rows {
            let mut row_map = Map::new();
            
            // Iteriramo čez stolpce sheme in vrednosti v vrstici hkrati
            for (i, col) in schema.columns.iter().enumerate() {
                if let Some(val) = row.get(i) {
                    let json_val = Self::value_to_json(val);
                    row_map.insert(col.name.clone(), json_val);
                }
            }
            
            json_rows.push(JsonValue::Object(row_map));
        }

        // Zavijemo v krovni objekt, ki da frontendu tudi nekaj metapodatkov
        let final_response = json!({
            "status": "success",
            "row_count": json_rows.len(),
            "columns": schema.columns.iter().map(|c| c.name.clone()).collect::<Vec<String>>(),
            "data": json_rows
        });

        // Pretvorimo v končni JSON string
        serde_json::to_string_pretty(&final_response).unwrap_or_else(|_| "{}".to_string())
    }
}