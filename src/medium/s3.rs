use crate::medium::Medium;
use crate::pipe::Reader;
use crate::storage::DataBlockId;
use crate::{pipe, PIPE_BUFFER_SIZE, TARGET_BLOCK_SIZE};
use aws_config::SdkConfig;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use std::future::Future;
use std::io::Read;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio::{runtime, task};
use tokio_util::io::SyncIoBridge;

/// Stores backups on S3-compatible object stores.
pub struct S3Medium {
    bucket: String,
    subdirectory: Option<String>,
    client: Client,
    tokio_handle: Runtime,
    tasks: Vec<JoinHandle<anyhow::Result<()>>>,
}

impl S3Medium {
    pub fn new(config: SdkConfig, bucket: &str, subdirectory: Option<&str>) -> Self {
        let client = Client::new(&config);
        let tokio_handle = runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .build()
            .expect("failed to build Tokio runtime");

        Self {
            bucket: bucket.to_owned(),
            subdirectory: subdirectory.map(str::to_owned),
            client,
            tokio_handle,
            tasks: Vec::new(),
        }
    }

    fn run_task(&mut self, task: impl Future<Output = anyhow::Result<()>> + Send + 'static) {
        let handle = self.tokio_handle.spawn(task);
        self.tasks.push(handle);
    }

    fn object_key_prefix(&self) -> String {
        match self.subdirectory.as_ref() {
            Some(s) => s.clone(),
            None => String::new(),
        }
    }

    fn index_object_key(&self) -> String {
        format!("{}/index", self.object_key_prefix())
    }

    fn data_block_object_key(&self, id: DataBlockId) -> String {
        format!("{}/data/{}", self.object_key_prefix(), id.to_hex())
    }
}

impl Medium for S3Medium {
    fn load_index(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        self.tokio_handle.block_on(async {
            let result = match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(self.index_object_key())
                .send()
                .await
            {
                Ok(r) => r,
                Err(SdkError::ServiceError(e))
                    if matches!(e.err(), GetObjectError::NoSuchKey(_)) =>
                {
                    return Ok(None)
                }
                Err(e) => return Err(e.into()),
            };
            let bytes = result.body.collect().await?;
            Result::<_, anyhow::Error>::Ok(Some(bytes.to_vec()))
        })
    }

    fn queue_save_index(&mut self, index_data: Vec<u8>) {
        let client = self.client.clone();
        let key = self.index_object_key();
        let bucket = self.bucket.clone();
        self.run_task(async move {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from(index_data))
                .send()
                .await?;
            Ok(())
        });
    }

    fn load_data_block(&mut self, id: DataBlockId) -> anyhow::Result<Option<Reader>> {
        let (writer, reader) = pipe::new(PIPE_BUFFER_SIZE);

        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.data_block_object_key(id);
        self.run_task(async move {
            let result = client.get_object().bucket(bucket).key(key).send().await?;

            let reader = SyncIoBridge::new(result.body.into_async_read());
            task::spawn_blocking(move || {
                writer.copy_from(reader);
            })
            .await?;

            Ok(())
        });

        Ok(Some(reader))
    }

    fn queue_save_data_block(&mut self, id: DataBlockId, mut data: Reader) {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.data_block_object_key(id);
        self.run_task(async move {
            // TODO support streaming (difficult because aws-sdk
            // requires retryability support)
            let data = task::spawn_blocking(move || {
                let mut buf = Vec::with_capacity(TARGET_BLOCK_SIZE.try_into().unwrap());
                data.read_to_end(&mut buf)?;
                Result::<_, anyhow::Error>::Ok(buf)
            })
            .await??;

            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from(data))
                .send()
                .await?;

            Ok(())
        });
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        for task in self.tasks.drain(..) {
            self.tokio_handle.block_on(task)??;
        }
        Ok(())
    }
}
