use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use std::error::Error;

async fn init_s3_client() -> Result<Client, Box<dyn Error>> {
    let creds = Credentials::new("minioadmin", "minioadmin", None, None, "local-credentials");

    let endpoints = [
        "http://minio:9000",
        "http://172.22.0.3:9000",
        "http://localhost:9000",
        "http://host.docker.internal:9000",
    ];

    for endpoint in endpoints {
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .credentials_provider(creds.clone())
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .build();

        match Client::from_conf(config.clone())
            .list_buckets()
            .send()
            .await
        {
            Ok(_) => return Ok(Client::from_conf(config.clone())),
            Err(e) => eprintln!("Failed to connect to {}: {:?}", endpoint, e),
        }
    }
    Err("All connection attempts failed".into())
}

pub async fn create_object(
    client: &Client,
    bucket: &str,
    key: &str,
    content: &str,
) -> Result<(), Box<dyn Error>> {
    let body = ByteStream::from(content.as_bytes().to_vec());
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await?;
    Ok(())
}

pub async fn read_object(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<String, Box<dyn Error>> {
    let response = client.get_object().bucket(bucket).key(key).send().await?;

    let bytes = response.body.collect().await?;
    let content = String::from_utf8(bytes.to_vec())?;
    Ok(content)
}

pub async fn delete_object(client: &Client, bucket: &str, key: &str) -> Result<(), Box<dyn Error>> {
    client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    Ok(())
}

pub async fn create_bucket(client: &Client, bucket: &str) -> Result<(), Box<dyn Error>> {
    client.create_bucket().bucket(bucket).send().await?;
    Ok(())
}

pub async fn delete_bucket(client: &Client, bucket_name: &str) -> Result<(), Box<dyn Error>> {
    // First empty the bucket (MinIO requires this)
    let objects = client.list_objects_v2().bucket(bucket_name).send().await?;

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
            client
                .delete_objects()
                .bucket(bucket_name)
                .delete(
                    Delete::builder()
                        .set_objects(Some(delete_objects))
                        .build()
                        .expect("Failed to build Delete object"),
                )
                .send()
                .await?;
        }
    }

    // Then delete the bucket
    client.delete_bucket().bucket(bucket_name).send().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_s3_operations() -> Result<(), Box<dyn Error>> {
        let client = init_s3_client().await?;
        let bucket = "test-bucket";
        let key = &format!("test-object-{}", Uuid::new_v4());
        let content = "Hello, local S3!";

        let _ = delete_bucket(&client, bucket).await;
        create_bucket(&client, bucket).await?;

        create_object(&client, bucket, key, content).await?;
        let read_content = read_object(&client, bucket, key).await?;
        assert_eq!(read_content, content);

        delete_object(&client, bucket, key).await?;
        assert!(read_object(&client, bucket, key).await.is_err());

        Ok(())
    }
}
