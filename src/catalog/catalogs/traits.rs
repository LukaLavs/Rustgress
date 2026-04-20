use super::super::schema::{Schema};
use crate::access::tuple::header::Tuple;

pub trait RGSomething {
    fn get_schema() -> Schema;
    fn make_tuple(self, schema: &Schema) -> Tuple;
    fn from_tuple(tuple: &Tuple) -> Self where Self: Sized;
    fn get_oid() -> u32;
}
