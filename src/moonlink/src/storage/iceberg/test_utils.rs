/// This module provides a few test util functions.
use crate::storage::iceberg::object_storage_catalog::{S3Catalog, S3CatalogConfig};

use std::sync::Arc;

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::head_bucket::HeadBucketError;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client as S3Client;
use iceberg::Error as IcebergError;
use iceberg::Result as IcebergResult;
use rand::random;
use randomizer::Randomizer;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;

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

#[allow(dead_code)]
pub(crate) fn get_test_minio_bucket_and_warehouse(
) -> (String /*bucket_name*/, String /*warehouse_url*/) {
    let random_string = Randomizer::ALPHANUMERIC(TEST_BUCKET_NAME_LEN)
        .string()
        .unwrap();
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

/// Create a S3 catalog, which communicates with local minio server.
#[allow(dead_code)]
pub(crate) fn create_minio_s3_catalog(bucket: &str, warehouse_uri: &str) -> S3Catalog {
    let config = S3CatalogConfig::new(
        /*warehouse_location=*/ warehouse_uri.to_string(),
        /*access_key_id=*/ MINIO_ACCESS_KEY_ID.to_string(),
        /*secret_access_key=*/ MINIO_SECRET_ACCESS_KEY.to_string(),
        /*region=*/ "auto".to_string(), // minio doesn't care about region.
        /*bucket=*/ bucket.to_string(),
        /*endpoint=*/ MINIO_ENDPOINT.to_string(),
    );

    println!("catalog config = {:?}", config);

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
async fn create_test_s3_bucket_impl(bucket: Arc<String>) -> IcebergResult<()> {
    let s3_client = create_s3_client().await;
    let exists = bucket_exists(&s3_client, &bucket).await?;
    if exists {
        return Ok(());
    }

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

#[allow(dead_code)]
pub(crate) async fn create_test_s3_bucket(bucket: String) -> IcebergResult<()> {
    let retry_strategy = ExponentialBackoff::from_millis(TEST_RETRY_INIT_MILLISEC)
        .map(jitter)
        .take(TEST_RETRY_COUNT);

    Retry::spawn(retry_strategy, {
        let bucket_name = Arc::new(bucket);
        move || {
            let bucket_name = Arc::clone(&bucket_name);
            async move { create_test_s3_bucket_impl(bucket_name).await }
        }
    })
    .await?;
    Ok(())
}

/// Delete test bucket in minio server.
#[allow(dead_code)]
async fn delete_test_s3_bucket_impl(bucket: Arc<String>) -> IcebergResult<()> {

    println!("attempt to delete bucket {}", bucket.clone());

    let s3_client = create_s3_client().await;
    // let exist = bucket_exists(&s3_client, &bucket).await?;

    // println!("bucket exists ? {}", exist);

    // if !exist {
    //     return Ok(());
    // }

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

        println!("list objexts ?");

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

            println!("deleted objects");

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

                println!("afetr deleted objects");
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

            println!("afetr deleted bucket");
    }

    println!("delete over!!!");

    Ok(())
}

#[allow(dead_code)]
pub(crate) async fn delete_test_s3_bucket(bucket: String) -> IcebergResult<()> {
    let retry_strategy = ExponentialBackoff::from_millis(TEST_RETRY_INIT_MILLISEC)
        .map(jitter)
        .take(TEST_RETRY_COUNT);

    Retry::spawn(retry_strategy, {
        let bucket_name = Arc::new(bucket);
        move || {
            let bucket_name = Arc::clone(&bucket_name);
            async move { delete_test_s3_bucket_impl(bucket_name).await }
        }
    })
    .await?;
    Ok(())
}
