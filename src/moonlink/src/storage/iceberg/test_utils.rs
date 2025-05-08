/// This module provides a few test util functions.
use crate::storage::iceberg::object_storage_catalog::{S3Catalog, S3CatalogConfig};

use randomizer::Randomizer;

/// Minio related constants.
///
/// Local minio warehouse needs special handling, so we simply prefix with special token.
#[allow(dead_code)]
pub(crate) static MINIO_TEST_BUCKET_PREFIX: &str = "test-minio-warehouse-";
#[allow(dead_code)]
pub(crate) static MINIO_TEST_WAREHOUSE_URI_PREFIX: &str = "s3://test-minio-warehouse-";
#[allow(dead_code)]
pub(crate) static MINIO_ACCESS_KEY_ID: &str = "minioadmin";
#[allow(dead_code)]
pub(crate) static MINIO_SECRET_ACCESS_KEY: &str = "minioadmin";
#[allow(dead_code)]
pub(crate) static MINIO_ENDPOINT: &str = "http://minio:9000";
#[allow(dead_code)]
static TEST_RETRY_COUNT: usize = 5;
#[allow(dead_code)]
static TEST_RETRY_INIT_MILLISEC: u64 = 100;
#[allow(dead_code)]
static TEST_BUCKET_NAME_LEN: usize = 10;

/// Create a S3 catalog, which communicates with local minio server.
#[allow(dead_code)]
pub(crate) fn create_minio_s3_catalog(bucket: &str, warehouse_uri: &str) -> S3Catalog {
    let config = S3CatalogConfig::new(
        /*warehouse_location=*/ "/tmp/iceberg-table".to_string(),
        /*access_key_id=*/ MINIO_ACCESS_KEY_ID.to_string(),
        /*secret_access_key=*/ MINIO_SECRET_ACCESS_KEY.to_string(),
        /*region=*/ "auto".to_string(), // minio doesn't care about region.
        /*bucket=*/ bucket.to_string(),
        /*endpoint=*/ MINIO_ENDPOINT.to_string(),
    );
    S3Catalog::new(config)
}

#[allow(dead_code)]
pub(crate) fn get_test_minio_bucket_and_warehouse(
) -> (String /*bucket_name*/, String /*warehouse_url*/) {
    // minio bucket name only allows lowercase case letters, digits and hyphen.
    let random_string = Randomizer::ALPHANUMERIC(TEST_BUCKET_NAME_LEN)
        .string()
        .unwrap()
        .to_lowercase();
    (
        format!("{}{}", MINIO_TEST_BUCKET_PREFIX, random_string),
        format!("{}{}", MINIO_TEST_WAREHOUSE_URI_PREFIX, random_string),
    )
}

#[allow(dead_code)]
pub(crate) fn get_test_minio_bucket(warehouse_uri: &str) -> String {
    let random_string = warehouse_uri
        .strip_prefix(MINIO_TEST_WAREHOUSE_URI_PREFIX)
        .unwrap()
        .to_string();
    format!("{}{}", MINIO_TEST_BUCKET_PREFIX, random_string)
}

#[cfg(test)]
pub(crate) mod catalog_test_utils {
    use std::sync::Arc;

    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type as IcebergType};
    use iceberg::Result as IcebergResult;
    use iceberg::{NamespaceIdent, TableCreation};

    // Test util function to get test table schema.
    pub(crate) fn create_test_table_schema() -> IcebergResult<Schema> {
        let field = NestedField::required(
            /*id=*/ 1,
            "field_name".to_string(),
            IcebergType::Primitive(PrimitiveType::Int),
        );
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![Arc::new(field)])
            .build()?;

        Ok(schema)
    }

    // Test util function to get table schema for the given table.
    pub(crate) fn create_test_table_creation(
        namespace_ident: &NamespaceIdent,
        table_name: &str,
    ) -> IcebergResult<TableCreation> {
        let schema = create_test_table_schema()?;
        let table_creation = TableCreation::builder()
            .name(table_name.to_string())
            .location(format!(
                "file:///tmp/iceberg-test/{}/{}",
                namespace_ident.to_url_string(),
                table_name
            ))
            .schema(schema.clone())
            .build();
        Ok(table_creation)
    }
}

#[cfg(test)]
pub(crate) mod object_store_test_utils {
    use super::*;

    use crate::storage::iceberg::tokio_retry_utils;

    use std::sync::Arc;

    use aws_sdk_s3::config::{Credentials, Region};
    use aws_sdk_s3::types::{Delete, ObjectIdentifier};
    use aws_sdk_s3::Client as S3Client;
    use iceberg::Error as IcebergError;
    use iceberg::Result as IcebergResult;
    use tokio_retry2::strategy::{jitter, ExponentialBackoff};
    use tokio_retry2::Retry;

    /// Create s3 client to connect minio.
    #[allow(dead_code)]
    async fn create_s3_client() -> S3Client {
        let creds = Credentials::new(
            MINIO_ACCESS_KEY_ID.to_string(),
            MINIO_SECRET_ACCESS_KEY.to_string(),
            /*session_token=*/ None,
            /*expires_after=*/ None,
            /*provider_name=*/ "local-credentials",
        );
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .credentials_provider(creds.clone())
            .region(Region::new("us-east-1"))
            .endpoint_url(MINIO_ENDPOINT.to_string())
            .force_path_style(true)
            .build();
        S3Client::from_conf(config.clone())
    }

    /// Create test bucket in minio server.
    #[allow(dead_code)]
    async fn create_test_s3_bucket_impl(bucket: Arc<String>) -> IcebergResult<()> {
        let s3_client = create_s3_client().await;
        s3_client
            .create_bucket()
            .bucket(bucket.to_string())
            .send()
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to create the test bucket in minio {}", e),
                )
            })?;
        Ok(())
    }

    /// Creates the provided bucket with exponential backoff retry; this function assumes the bucket doesn't exist, otherwise it will return error.
    #[allow(dead_code)]
    pub(crate) async fn create_test_s3_bucket(bucket: String) -> IcebergResult<()> {
        let retry_strategy = ExponentialBackoff::from_millis(TEST_RETRY_INIT_MILLISEC)
            .map(jitter)
            .take(TEST_RETRY_COUNT);

        Retry::spawn(retry_strategy, {
            let bucket_name = Arc::new(bucket);
            move || {
                let bucket_name = Arc::clone(&bucket_name);
                async move {
                    create_test_s3_bucket_impl(bucket_name)
                        .await
                        .map_err(tokio_retry_utils::iceberg_to_tokio_retry_error)
                }
            }
        })
        .await?;
        Ok(())
    }

    /// Delete test bucket in minio server.
    #[allow(dead_code)]
    async fn delete_test_s3_bucket_impl(bucket: Arc<String>) -> IcebergResult<()> {
        let s3_client = create_s3_client().await;
        let objects = s3_client
            .list_objects_v2()
            .bucket(bucket.to_string())
            .send()
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to list objects under test bucket in minio {}", e),
                )
            })?;

        if let Some(contents) = objects.contents {
            let delete_objects: Vec<ObjectIdentifier> = contents
                .into_iter()
                .filter_map(|o| o.key)
                .map(|key| {
                    ObjectIdentifier::builder()
                        .key(key)
                        .build()
                        .expect("Failed to build ObjectIdentifier")
                })
                .collect();

            if !delete_objects.is_empty() {
                s3_client
                    .delete_objects()
                    .bucket(bucket.to_string())
                    .delete(
                        Delete::builder()
                            .set_objects(Some(delete_objects))
                            .build()
                            .expect("Failed to build Delete object"),
                    )
                    .send()
                    .await
                    .map_err(|e| {
                        IcebergError::new(
                            iceberg::ErrorKind::Unexpected,
                            format!("Failed to delete objects under test bucket in minio {}", e),
                        )
                    })?;
            }

            s3_client
                .delete_bucket()
                .bucket(bucket.to_string())
                .send()
                .await
                .map_err(|e| {
                    IcebergError::new(
                        iceberg::ErrorKind::Unexpected,
                        format!("Failed to delete the test bucket in minio {}", e),
                    )
                })?;
        }

        Ok(())
    }

    /// Delete the provided bucket with exponential backoff retry; this function assume bucket already exists and return error if not.
    #[allow(dead_code)]
    pub(crate) async fn delete_test_s3_bucket(bucket: String) -> IcebergResult<()> {
        let retry_strategy = ExponentialBackoff::from_millis(TEST_RETRY_INIT_MILLISEC)
            .map(jitter)
            .take(TEST_RETRY_COUNT);

        Retry::spawn(retry_strategy, {
            let bucket_name = Arc::new(bucket);
            move || {
                let bucket_name = Arc::clone(&bucket_name);
                async move {
                    delete_test_s3_bucket_impl(bucket_name)
                        .await
                        .map_err(tokio_retry_utils::iceberg_to_tokio_retry_error)
                }
            }
        })
        .await?;
        Ok(())
    }
}
