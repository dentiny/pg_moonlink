/// This module provides a few test util functions.

use crate::storage::iceberg::object_storage_catalog::{S3Catalog, S3CatalogConfig};

use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{Credentials, Region};
use iceberg::Result as IcebergResult;
use iceberg::Error as IcebergError;

/// Minio bucket for object storage catalog testing.
pub(crate) static S3_TEST_BUCKET: &str = "test-bucket";
pub(crate) static S3_TEST_WAREHOUSE_URI: &str = "s3://test-bucket";

/// Create a S3 catalog, which communicates with local minio server.
pub(crate) fn create_minio_s3_catalog() -> S3Catalog {
    let config = S3CatalogConfig::new(
        /*warehouse_location=*/ S3_TEST_WAREHOUSE_URI.to_string(),
        /*access_key_id=*/ "minioadmin".to_string(),
        /*secret_access_key=*/ "minioadmin".to_string(),
        /*region=*/ "auto".to_string(), // minio doesn't care about region.
        /*bucket=*/ S3_TEST_BUCKET.to_string(),
        /*endpoint=*/ "http://minio:9000".to_string(),
    );
    S3Catalog::new(config)
}

/// Create s3 client to connect minio.
async fn create_s3_client() -> S3Client {
    let creds = Credentials::new("minioadmin", "minioadmin", None, None, "local-credentials");
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .credentials_provider(creds.clone())
        .region(Region::new("us-east-1"))
        .endpoint_url("http://minio:9000")
        .force_path_style(true)
        .build();
    S3Client::from_conf(config.clone())
}

/// Create test bucket in minio server.
pub(crate) async fn create_test_s3_bucket() -> IcebergResult<()> {
    let s3_client = create_s3_client().await;
    // s3_client.create_bucket().bucket(S3_TEST_BUCKET.to_string()).send()
    //     .await
    //     .map_err(|e| {
    //         IcebergError::new(
    //             iceberg::ErrorKind::Unexpected,
    //             format!("Failed to create bucket: {}", e),
    //         )
    //     })?;

    s3_client.create_bucket().bucket(S3_TEST_BUCKET.to_string()).send()
        .await.ok();

    Ok(())
}    
