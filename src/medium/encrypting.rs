use crate::{
    medium::Medium,
    model::BlockId,
    streaming_crypto::{ReadEncrypted, WriteEncrypted},
};
use aead::generic_array::GenericArray;
use aes_gcm_siv::{Aes256GcmSiv, Key};
use anyhow::anyhow;
use argon2::Argon2;
use chrono::{DateTime, Utc};
use std::{
    io,
    io::{Cursor, Read, Write},
};

/// A layer that optionally encrypts the data written to the medium.
pub struct EncryptingMedium<M> {
    inner: M,
    /// `None` if encryption is disabled
    key: Option<Key<Aes256GcmSiv>>,
}

impl<M> EncryptingMedium<M> {
    pub fn new(inner: M, key: Option<Key<Aes256GcmSiv>>) -> Self {
        Self { inner, key }
    }

    pub fn with_password(inner: M, password: Option<&str>) -> Self {
        let key = password.map(derive_key);
        Self::new(inner, key)
    }

    fn wrap_reader<'a>(
        &self,
        mut reader: impl Read + Send + 'a,
    ) -> anyhow::Result<Box<dyn Read + Send + 'a>> {
        let header = decode_header(&mut reader)?;
        if header.encrypted {
            let key = self
                .key
                .as_ref()
                .ok_or_else(|| anyhow!("backup is encrypted; please provide a key"))?;
            let wrapper = ReadEncrypted::new(reader, key, &GenericArray::from(header.nonce));
            Ok(Box::new(wrapper))
        } else {
            Ok(Box::new(reader))
        }
    }

    fn wrap_writer<'a>(
        &self,
        mut writer: impl Write + Send + 'a,
    ) -> anyhow::Result<Box<dyn Write + Send + 'a>> {
        match &self.key {
            Some(key) => {
                let nonce = generate_nonce();
                let header = Header {
                    encrypted: true,
                    nonce,
                };
                encode_header(&header, &mut writer)?;

                let wrapper = WriteEncrypted::new(writer, key, &GenericArray::from(nonce));
                Ok(Box::new(wrapper))
            }
            None => {
                let header = Header::unencrypted();
                encode_header(&header, &mut writer)?;
                Ok(Box::new(writer))
            }
        }
    }
}

impl<M> Medium for EncryptingMedium<M>
where
    M: Medium,
{
    fn load_version(&self, n: u64) -> anyhow::Result<Option<Vec<u8>>> {
        match self.inner.load_version(n) {
            Ok(Some(encrypted)) => {
                let mut decrypted = Vec::new();
                self.wrap_reader(Cursor::new(encrypted))?
                    .read_to_end(&mut decrypted)?;
                Ok(Some(decrypted))
            }
            result => result,
        }
    }

    fn save_version(
        &self,
        unencrypted_version_bytes: Vec<u8>,
        timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let mut encrypted_version_bytes = Vec::new();

        {
            let mut writer = self.wrap_writer(&mut encrypted_version_bytes)?;
            writer.write_all(&unencrypted_version_bytes)?;
        }

        self.inner.save_version(encrypted_version_bytes, timestamp)
    }

    fn load_block(&self, block_id: BlockId) -> anyhow::Result<Box<dyn Read + Send>> {
        self.inner
            .load_block(block_id)
            .and_then(|reader| self.wrap_reader(reader))
    }

    fn save_block(&self, block_id: BlockId) -> anyhow::Result<Box<dyn Write + Send>> {
        self.inner
            .save_block(block_id)
            .and_then(|writer| self.wrap_writer(writer))
    }

    fn flush(&self) -> anyhow::Result<()> {
        self.inner.flush()
    }
}

pub fn derive_key(password: &str) -> Key<Aes256GcmSiv> {
    let mut key = [0u8; 32];
    // TODO: evaluate if using a random salt is desired for security here,
    // given that using nonces should already have the same effect
    let salt = b"look on my works, ye mighty, and despair";
    Argon2::default()
        .hash_password_into(password.as_bytes(), salt, &mut key)
        .expect("failed to derive key from password");
    GenericArray::from(key)
}

struct Header {
    encrypted: bool,
    nonce: [u8; 8],
}

impl Header {
    pub fn unencrypted() -> Self {
        Self {
            encrypted: false,
            nonce: Default::default(),
        }
    }
}

fn decode_header(mut reader: impl Read) -> io::Result<Header> {
    let mut is_encrypted = [0];
    reader.read_exact(&mut is_encrypted)?;
    let mut nonce = [0; 8];
    reader.read_exact(&mut nonce)?;
    Ok(Header {
        nonce,
        encrypted: is_encrypted[0] != 0,
    })
}

fn encode_header(header: &Header, mut writer: impl Write) -> io::Result<()> {
    writer.write_all(&[header.encrypted as u8])?;
    writer.write_all(&header.nonce)?;
    Ok(())
}

fn generate_nonce() -> [u8; 8] {
    // Not using OsRng is okay here since nonces
    // do not have to be strictly unique in AES-GCM-SIV.
    // StdRng is secure enough.
    rand::random()
}
