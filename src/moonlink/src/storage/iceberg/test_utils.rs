/// This module provides a few test util functions.
use crate::storage::iceberg::object_storage_catalog::{S3Catalog, S3CatalogConfig};

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::head_bucket::HeadBucketError;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client as S3Client;
use iceberg::Error as IcebergError;
use iceberg::Result as IcebergResult;

/// Minio related constants.
#[allow(dead_code)]
pub(crate) static MINIO_TEST_BUCKET: &str = "test-bucket";
#[allow(dead_code)]
pub(crate) static MINIO_TEST_WAREHOUSE_URI: &str = "s3://test-bucket";
#[allow(dead_code)]
pub(crate) static MINIO_ACCESS_KEY_ID: &str = "minioadmin";
#[allow(dead_code)]
pub(crate) static MINIO_SECRET_ACCESS_KEY: &str = "minioadmin";
#[allow(dead_code)]
pub(crate) static MINIO_ENDPOINT: &str = "http://minio:9000";

/// Create a S3 catalog, which communicates with local minio server.
#[allow(dead_code)]
pub(crate) fn create_minio_s3_catalog() -> S3Catalog {
    let config = S3CatalogConfig::new(
        /*warehouse_location=*/ MINIO_TEST_WAREHOUSE_URI.to_string(),
        /*access_key_id=*/ MINIO_ACCESS_KEY_ID.to_string(),
        /*secret_access_key=*/ MINIO_SECRET_ACCESS_KEY.to_string(),
        /*region=*/ "auto".to_string(), // minio doesn't care about region.
        /*bucket=*/ MINIO_TEST_BUCKET.to_string(),
        /*endpoint=*/ MINIO_ENDPOINT.to_string(),
    );
    S3Catalog::new(config)
}

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

/// Check if a bucket exists in minio server.
async fn bucket_exists(s3_client: &S3Client, bucket: &str) -> IcebergResult<bool> {
    match s3_client.head_bucket().bucket(bucket).send().await {
        Ok(_) => Ok(true),
        Err(err) => {
            if let SdkError::ServiceError(service_err) = err {
                match service_err.err() {
                    HeadBucketError::NotFound(_) => Ok(false),
                    _ => Ok(true),
                }
            } else {
                Err(IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to check bucket existence: {}", err),
                ))
            }
        }
    }
}

/// Create test bucket in minio server.
#[allow(dead_code)]
pub(crate) async fn create_test_s3_bucket() -> IcebergResult<()> {
    let s3_client = create_s3_client().await;
    let exists = bucket_exists(&s3_client, MINIO_TEST_BUCKET).await?;
    if exists {
        return Ok(());
    }

    s3_client
        .create_bucket()
        .bucket(MINIO_TEST_BUCKET.to_string())
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

/// Delete test bucket in minio server.
#[allow(dead_code)]
pub(crate) async fn delete_test_s3_bucket() -> IcebergResult<()> {
    let s3_client = create_s3_client().await;
    let exist = bucket_exists(&s3_client, MINIO_TEST_BUCKET).await?;
    if !exist {
        return Ok(());
    }

    let objects = s3_client
        .list_objects_v2()
        .bucket(MINIO_TEST_BUCKET.to_string())
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
                .bucket(MINIO_TEST_BUCKET.to_string())
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
            .bucket(MINIO_TEST_BUCKET.to_string())
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
