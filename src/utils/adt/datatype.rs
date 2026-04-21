use crate::utils::adt::traits::TypeDescriptor;

macro_rules! register_types {
    ($( $variant:ident => $struct:ty ),*) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum DataType {
            $( $variant, )*
        }

        #[derive(Debug, Clone, PartialEq)]
        pub enum Value {
            $( $variant(<$struct as TypeDescriptor>::Native), )*
            Null,
        }

        impl DataType {
            pub fn from_oid(oid: u32) -> Self {
                match oid {
                    $( <$struct>::OID => DataType::$variant, )*
                    _ => panic!("Unknown OID: {}", oid),
                }
            }
            pub fn unpack(&self, data: &[u8], cursor: &mut usize) -> Value {
                match self {
                    $( DataType::$variant => Value::$variant(<$struct>::unpack(data, cursor)), )*
                }
            }
            pub fn type_definitions() -> Vec<(u32, &'static str, i32, bool)> {
                vec![
                    $( ( <$struct>::OID, <$struct>::NAME, <$struct>::BYTE_LEN, <$struct>::IS_FIXED ), )*
                ]
            }
            pub fn name(&self) -> &'static str {
                match self {
                    $( DataType::$variant => <$struct>::NAME, )*
                }
            }
        }

        pub trait FromValue: TypeDescriptor {
            fn from_value(val: &Value) -> Option<Self::Native>;
        }
        $(
            impl FromValue for $struct {
                fn from_value(val: &Value) -> Option<Self::Native> {
                    if let Value::$variant(v) = val {
                        Some(v.clone())
                    } else {
                        None
                    }
                }
            }
        )*

        
        impl Value {
            pub fn pack(&self, buffer: &mut Vec<u8>) {
                match self {
                    $( Value::$variant(v) => <$struct>::pack(v, buffer), )*
                    Value::Null => {},
                }
            }
            pub fn as_native<T: FromValue>(&self) -> Option<T::Native> {
                T::from_value(self)
            }
            pub fn as_str(&self) -> String {
                match self {
                    $( Value::$variant(v) => <$struct>::to_string(v), )*
                    Value::Null => "NULL".to_string(),
                }
            }
        }
    };


}


register_types! {
    Integer   => crate::utils::adt::integer::IntegerType,
    Boolean   => crate::utils::adt::boolean::BooleanType,
    Varchar   => crate::utils::adt::varchar::VarcharType,
    Timestamp => crate::utils::adt::timestamp::TimestampType,
    Float     => crate::utils::adt::float::FloatType,
    Double    => crate::utils::adt::double::DoubleType,
    Numeric   => crate::utils::adt::numeric::NumericType
    // add new types here
}