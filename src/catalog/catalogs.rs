use crate::storage::disk::manager::Table;
use super::types::{DataType};
use super::rg_attribute::RGAttribute;
use super::rg_class::RGClass;



pub fn bootstrap_system_catalogs() {
    if std::path::Path::new("data/rg_class.db").exists() {
        println!("Catalogs exist already.");
        return;
    }
    std::fs::create_dir_all("data").expect("Folder data could not be created!");


    // CREATE rg_class TABLE
    let rg_class_schema = RGClass::get_schema();
    let mut rg_class_table = Table::open("data/rg_class.db");

    let to_insert = [
        RGClass::new(1, "rg_class".to_string(), 1,
            1, 2.),
        RGClass::new(2, "rg_attributes".to_string(), 1,
            1, 2.),
    ];
    for row in to_insert {
        rg_class_table.insert_tuple(&row.make_tuple(&rg_class_schema));
    }


    // CREATE rg_attributes TABLE
    let rg_attribute_schema = RGAttribute::get_schema();
    let mut rg_attribute_table = Table::open("data/rg_attributes.db");
    let to_insert = [
        // rg_class table
        RGAttribute::new(DataType::Integer, 1, 
            "oid".to_string(), 1, ),
        RGAttribute::new(DataType::Varchar(64),1,
            "relname".to_string(), 2),
        RGAttribute::new(DataType::Integer, 1, 
            "relnamespace".to_string(), 3),
        RGAttribute::new(DataType::Integer, 1,
            "relpages".to_string(), 4),
        RGAttribute::new(DataType::Float, 1,
            "reltuples".to_string(), 5),

        // rg_attributes table
        RGAttribute::new(DataType::Integer, 2,
            "attrelid".to_string(), 1),
        RGAttribute::new(DataType::Varchar(64), 2,
            "attname".to_string(), 2),
        RGAttribute::new(DataType::Integer, 2, 
            "atttypid".to_string(), 3),
        RGAttribute::new(DataType::Integer, 2,
            "attnum".to_string(), 4),
        RGAttribute::new(DataType::Integer, 2,
            "attlen".to_string(), 5),
    ];

    for row in to_insert {
        rg_attribute_table.insert_tuple(&row.make_tuple(&rg_attribute_schema));
    }

    
    println!("rg_class.db and rg_attributes.db successfully created.");
}