use super::{version_timestamp_from_file_name, BLOCKS_DIR, VERSIONS_DIR};
use crate::{
    medium::{version_file_name, Medium},
    model::BlockId,
    pipe, IO_BUFFER_SIZE,
};
use anyhow::Context;
use aws_config::SdkConfig;
use aws_sdk_s3::{primitives::ByteStream, Client};
use chrono::{DateTime, Utc};
use pollster::FutureExt;
use std::{
    cmp,
    future::Future,
    io,
    io::{Read, Write},
    sync::Mutex,
};
use tokio::{io::AsyncReadExt, runtime, runtime::Runtime, task::JoinSet};

/// Medium that stores data on an S3-compatible object store.
pub struct S3Medium {
    bucket: String,
    subdirectory: String,
    s3_client: Client,
    tokio_handle: Runtime,

    tasks_needing_wait: Mutex<JoinSet<()>>,
}

impl S3Medium {
    pub fn new(config: &SdkConfig, bucket: &str, backup_name: &str) -> Self {
        let s3_client = Client::new(config);
        let tokio_handle = runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .build()
            .expect("failed to build Tokio runtime");

        Self {
            bucket: bucket.to_owned(),
            subdirectory: backup_name.to_owned(),
            s3_client,
            tokio_handle,
            tasks_needing_wait: Default::default(),
        }
    }

    fn versions_dir_prefix(&self) -> String {
        format!("{}/{VERSIONS_DIR}", self.subdirectory)
    }

    fn object_key_for_version(&self, version_ts: DateTime<Utc>) -> String {
        format!(
            "{}/{VERSIONS_DIR}/{}",
            self.subdirectory,
            version_file_name(version_ts)
        )
    }

    fn version_timestamp_from_object_key(&self, key: &str) -> anyhow::Result<DateTime<Utc>> {
        let parts: Vec<&str> = key.split('/').collect();
        let file_name = parts
            .last()
            .context("expected at least one part in object key")?;
        version_timestamp_from_file_name(file_name)
    }

    fn object_key_for_block(&self, block_id: BlockId) -> String {
        format!("{}/{BLOCKS_DIR}/{block_id}", self.subdirectory)
    }

    fn spawn_task(&self, task: impl Future<Output = ()> + Send + 'static) {
        let _guard = self.tokio_handle.enter();

        const MAX_PARALLEL_TASKS: usize = 16;
        let mut tasks_needing_wait = self.tasks_needing_wait.lock().unwrap();

        while tasks_needing_wait.try_join_next().is_some() {}

        if tasks_needing_wait.len() >= MAX_PARALLEL_TASKS {
            // apply backpressure by flushing a task
            tasks_needing_wait.join_next().block_on();
        }
        tasks_needing_wait.spawn(task);
    }
}

impl Medium for S3Medium {
    fn load_version(&self, n: u64) -> anyhow::Result<Option<Vec<u8>>> {
        // List all objects in bucket with pagination
        let mut objects = Vec::new();
        let mut cont_token = None;
        loop {
            let mut builder = self
                .s3_client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(self.versions_dir_prefix());
            if let Some(token) = &cont_token {
                builder = builder.continuation_token(token);
            }
            let response = self.tokio_handle.block_on(builder.send())?;

            objects.extend(response.contents.unwrap_or_default());

            cont_token = response.next_continuation_token;
            if cont_token.is_none() {
                break;
            }
        }

        let mut versions: Vec<_> = objects
            .into_iter()
            .map(|object| {
                let key = object.key.context("missing object key")?;
                let timestamp = self.version_timestamp_from_object_key(&key)?;
                Result::<_, anyhow::Error>::Ok((timestamp, key))
            })
            .collect::<Result<_, anyhow::Error>>()?;
        versions.sort_by_key(|(date, _)| cmp::Reverse(*date));

        if n as usize >= versions.len() {
            return Ok(None);
        }

        let (_, key) = &versions[n as usize];

        let response = self.tokio_handle.block_on(
            self.s3_client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send(),
        )?;
        let bytes = self.tokio_handle.block_on(response.body.collect())?;
        Ok(Some(bytes.to_vec()))
    }

    fn save_version(&self, version_bytes: Vec<u8>, timestamp: DateTime<Utc>) -> anyhow::Result<()> {
        let key = self.object_key_for_version(timestamp);
        self.tokio_handle.block_on(
            self.s3_client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(ByteStream::from(version_bytes))
                .send(),
        )?;
        Ok(())
    }

    fn load_block(&self, block_id: BlockId) -> anyhow::Result<Box<dyn Read + Send>> {
        let (mut writer, reader) = pipe::new(IO_BUFFER_SIZE);

        let s3_client = self.s3_client.clone();
        let bucket = self.bucket.clone();
        let key = self.object_key_for_block(block_id);

        self.spawn_task(async move {
            if let Err(e) = stream_object_from_s3(&mut writer, &s3_client, &bucket, &key).await {
                writer.disconnect_with_error(anyhow_error_to_io(e));
            }
        });
        Ok(Box::new(reader))
    }

    fn save_block(&self, block_id: BlockId) -> anyhow::Result<Box<dyn Write + Send>> {
        let (writer, mut reader) = pipe::new(IO_BUFFER_SIZE);

        let s3_client = self.s3_client.clone();
        let bucket = self.bucket.clone();
        let key = self.object_key_for_block(block_id);

        self.spawn_task(async move {
            if let Err(e) = stream_object_to_s3(&mut reader, &s3_client, &bucket, &key).await {
                reader.disconnect_with_error(anyhow_error_to_io(e));
            }
        });
        Ok(Box::new(writer))
    }

    fn flush(&self) -> anyhow::Result<()> {
        let mut tasks_needing_wait = self.tasks_needing_wait.lock().unwrap();
        while tasks_needing_wait.join_next().block_on().is_some() {}
        Ok(())
    }
}

async fn stream_object_from_s3(
    pipe: &mut pipe::Writer,
    client: &Client,
    bucket: &str,
    key: &str,
) -> anyhow::Result<()> {
    let response = client.get_object().bucket(bucket).key(key).send().await?;
    let response_reader = response.body.into_async_read();
    pipe.async_copy_all_from_reader(response_reader).await?;
    Ok(())
}

async fn stream_object_to_s3(
    pipe: &mut pipe::Reader,
    client: &Client,
    bucket: &str,
    key: &str,
) -> anyhow::Result<()> {
    // To enable retriability, we unfortunately need to
    // collect the whole buffer into memory.
    let mut buf = Vec::new();
    AsyncReadExt::read_to_end(pipe, &mut buf).await?;
    let body = ByteStream::from(buf);

    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await?;

    Ok(())
}

fn anyhow_error_to_io(anyhow: anyhow::Error) -> io::Error {
    anyhow
        .downcast::<io::Error>()
        .unwrap_or_else(|anyhow| io::Error::new(io::ErrorKind::Other, anyhow))
}
