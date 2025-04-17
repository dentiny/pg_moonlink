use crate::error::Result;
use crate::storage::mooncake_table::Snapshot;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::spec::{
    NestedField, NestedFieldRef, PrimitiveType, Schema as IcebergSchema, SchemaId, StructType,
    Type as IcebergType,
};
use iceberg::table::Table as IcebergTable;
use iceberg::table::TableBuilder as IcebergTableBuilder;
use iceberg::NamespaceIdent;
use iceberg::TableCreation;
use iceberg::{Catalog, Result as IcebergResult, TableIdent};
use iceberg_catalog_memory::MemoryCatalog;

// UNDONE(Iceberg):

fn arrow_to_iceberg_schema(arrow_schema: &ArrowSchema) -> IcebergSchema {
    let mut field_id_counter = 1;

    let iceberg_fields: Vec<NestedFieldRef> = arrow_schema
        .fields
        .iter()
        .map(|f| {
            let iceberg_type = arrow_type_to_iceberg_type(f.data_type());
            let field = if f.is_nullable() {
                NestedField::optional(field_id_counter, f.name().clone(), iceberg_type)
            } else {
                NestedField::required(field_id_counter, f.name().clone(), iceberg_type)
            };
            field_id_counter += 1;
            Arc::new(field)
        })
        .collect();

    IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(iceberg_fields)
        .build()
        .expect("Failed to build Iceberg schema")
}

fn arrow_type_to_iceberg_type(data_type: &ArrowDataType) -> IcebergType {
    match data_type {
        ArrowDataType::Boolean => IcebergType::Primitive(PrimitiveType::Boolean),
        ArrowDataType::Int32 => IcebergType::Primitive(PrimitiveType::Int),
        ArrowDataType::Int64 => IcebergType::Primitive(PrimitiveType::Long),
        ArrowDataType::Float32 => IcebergType::Primitive(PrimitiveType::Float),
        ArrowDataType::Float64 => IcebergType::Primitive(PrimitiveType::Double),
        ArrowDataType::Utf8 => IcebergType::Primitive(PrimitiveType::String),
        ArrowDataType::Binary => IcebergType::Primitive(PrimitiveType::Binary),
        ArrowDataType::Timestamp(_, _) => IcebergType::Primitive(PrimitiveType::Timestamp),
        ArrowDataType::Date32 => IcebergType::Primitive(PrimitiveType::Date),
        // Add more mappings as needed
        _ => panic!("Unsupported Arrow data type: {:?}", data_type),
    }
}

async fn get_or_create_iceberg_table(
    namespace: &[&str],
    table_name: &str,
    arrow_schema: &ArrowSchema,
) -> IcebergResult<IcebergTable> {
    // 1. Build file I/O
    let file_io = FileIOBuilder::new("memory").build()?;

    // 2. Create a memory catalog
    let mut catalog = MemoryCatalog::new(file_io.clone(), None);

    // 3. Build table identifier
    let mut namespace_ident_vec = namespace.to_vec();
    namespace_ident_vec.push(table_name);
    let table_ident: TableIdent = TableIdent::from_strs(namespace_ident_vec.clone())?;

    let namespace_ident =
        NamespaceIdent::from_vec(namespace_ident_vec.iter().map(|s| s.to_string()).collect())?;

    // 4. Try to load the table
    match catalog.load_table(&table_ident).await {
        Ok(table) => Ok(table),
        Err(_) => {
            // 5. Create the table if it doesn't exist
            let iceberg_schema = arrow_to_iceberg_schema(arrow_schema);

            let tbl_creation = TableCreation::builder()
                .name(table_name.to_string())
                .schema(iceberg_schema)
                .properties(HashMap::new())
                .build();

            let table = catalog.create_table(&namespace_ident, tbl_creation).await?;
            Ok(table)
        }
    }
}

pub trait IcebergSnapshot {
    // Write the current version to iceberg
    async fn _export_to_iceberg(&self) -> Result<()>;

    // Create a snapshot by reading from iceberg
    fn _load_from_iceberg(&self) -> Self;
}

impl IcebergSnapshot for Snapshot {
    async fn _export_to_iceberg(&self) -> Result<()> {
        todo!()
    }

    fn _load_from_iceberg(&self) -> Self {
        todo!()
    }
}
