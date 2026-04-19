use super::super::schema::{Schema};
use crate::access::tuple::header::Tuple;

pub trait RGSomething {
    fn get_schema() -> Schema;
    fn make_tuple(self, schema: &Schema) -> Tuple;
    fn get_oid() -> u32;
}
