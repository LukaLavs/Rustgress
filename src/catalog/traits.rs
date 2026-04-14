use super::schema::{Schema, Column};
use super::types::{DataType, Value};
use crate::access::tuple::header::Tuple;

pub trait RGSomething {
    fn get_schema() -> Schema;
    fn make_tuple(&self, schema: &Schema) -> Tuple;
}