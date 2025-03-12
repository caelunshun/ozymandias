use crate::KiB;
use aead::{
    stream::{DecryptorLE31, EncryptorLE31, Nonce, StreamLE31},
    Key,
};
use aes_gcm_siv::Aes256GcmSiv;
use std::{
    io,
    io::{Read, Write},
    iter,
};

const APPROX_MESSAGE_SIZE: usize = 128 * KiB;
const LENGTH_PREFIX_SIZE: usize = 4;
const EOF_INDICATOR: usize = u32::MAX as usize;

fn encode_length_prefix(len: usize) -> [u8; 4] {
    (u32::try_from(len).unwrap()).to_le_bytes()
}

fn decode_length_prefix(encoded: [u8; 4]) -> usize {
    u32::from_le_bytes(encoded).try_into().unwrap()
}

/// High-level utility to perform AES-GCM-SIV stream encryption
/// on data written to the underlying writer.
pub struct WriteEncrypted<W: Write> {
    buffer: Vec<u8>,
    inner: W,
    encryptor: Option<EncryptorLE31<Aes256GcmSiv>>,
}

impl<W> WriteEncrypted<W>
where
    W: Write,
{
    pub fn new(
        inner: W,
        key: &Key<Aes256GcmSiv>,
        nonce: &Nonce<Aes256GcmSiv, StreamLE31<Aes256GcmSiv>>,
    ) -> Self {
        Self {
            buffer: Vec::new(),
            inner,
            encryptor: Some(EncryptorLE31::new(key, nonce)),
        }
    }

    fn flush_message(&mut self, is_last: bool) -> io::Result<()> {
        let result = if is_last {
            let encryptor = self.encryptor.take().unwrap();
            encryptor.encrypt_last_in_place(&[], &mut self.buffer)
        } else {
            let encryptor = self.encryptor.as_mut().unwrap();
            encryptor.encrypt_next_in_place(&[], &mut self.buffer)
        };
        result.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let length = if is_last {
            EOF_INDICATOR
        } else {
            self.buffer.len()
        };
        let length_prefix = encode_length_prefix(length);
        self.inner.write_all(&length_prefix)?;
        self.inner.write_all(&self.buffer)?;
        self.buffer.clear();
        Ok(())
    }

    #[allow(unused)]
    pub fn finish(mut self) -> io::Result<()> {
        self.finish_inner()
    }

    fn finish_inner(&mut self) -> io::Result<()> {
        self.flush_message(true)?;
        self.inner.flush()?;
        Ok(())
    }
}

impl<W> Drop for WriteEncrypted<W>
where
    W: Write,
{
    fn drop(&mut self) {
        if self.encryptor.is_some() {
            self.finish_inner().ok();
        }
    }
}

impl<W> Write for WriteEncrypted<W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);

        if self.buffer.len() >= APPROX_MESSAGE_SIZE {
            self.flush_message(false)?;
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.flush_message(false)?;
        self.inner.flush()
    }
}

/// High-level utility to perform AES-GCM-SIV stream decryption
/// on data read from an underlying reader.
pub struct ReadEncrypted<R> {
    encrypted_buffer: Vec<u8>,
    decrypted_buffer: Vec<u8>,
    next_message_size: Option<usize>,
    inner: R,
    decryptor: Option<DecryptorLE31<Aes256GcmSiv>>,
    is_eof: bool,
}

impl<R> ReadEncrypted<R> {
    pub fn new(
        inner: R,
        key: &Key<Aes256GcmSiv>,
        nonce: &Nonce<Aes256GcmSiv, StreamLE31<Aes256GcmSiv>>,
    ) -> Self {
        Self {
            encrypted_buffer: Vec::new(),
            decrypted_buffer: Vec::new(),
            next_message_size: None,
            inner,
            decryptor: Some(DecryptorLE31::new(key, nonce)),
            is_eof: false,
        }
    }
}

impl<R> ReadEncrypted<R>
where
    R: Read,
{
    fn decrypt_next_message(&mut self, next_message_size: usize) -> io::Result<()> {
        let encrypted_len = if next_message_size == EOF_INDICATOR {
            self.encrypted_buffer.len()
        } else {
            next_message_size
        };

        let decrypted = if next_message_size == EOF_INDICATOR {
            let decryptor = self.decryptor.take().unwrap();
            decryptor.decrypt_last(&self.encrypted_buffer[..encrypted_len])
        } else {
            let decryptor = self.decryptor.as_mut().unwrap();
            decryptor.decrypt_next(&self.encrypted_buffer[..encrypted_len])
        };
        let decrypted =
            decrypted.map_err(|_| io::Error::new(io::ErrorKind::Other, "could not decrypt"))?;

        self.encrypted_buffer.drain(..encrypted_len);

        self.decrypted_buffer.extend_from_slice(&decrypted);

        if next_message_size == EOF_INDICATOR {
            self.is_eof = true;
        }
        self.next_message_size = None;

        Ok(())
    }
}

impl<R> Read for ReadEncrypted<R>
where
    R: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.decrypted_buffer.is_empty() {
            let next_message_size = match self.next_message_size {
                Some(l) => l,
                None => {
                    if self.is_eof {
                        EOF_INDICATOR
                    } else {
                        if self.encrypted_buffer.len() < LENGTH_PREFIX_SIZE {
                            let cursor = self.encrypted_buffer.len();
                            self.encrypted_buffer.extend(
                                iter::repeat(0)
                                    .take(LENGTH_PREFIX_SIZE - self.encrypted_buffer.len()),
                            );
                            self.inner
                                .read_exact(&mut self.encrypted_buffer[cursor..])?;
                        }

                        let length = decode_length_prefix(
                            self.encrypted_buffer[..LENGTH_PREFIX_SIZE]
                                .try_into()
                                .unwrap(),
                        );
                        self.next_message_size = Some(length);
                        self.encrypted_buffer.drain(..LENGTH_PREFIX_SIZE);
                        length
                    }
                }
            };

            if next_message_size == EOF_INDICATOR {
                if self.is_eof {
                    break;
                } else {
                    self.inner.read_to_end(&mut self.encrypted_buffer)?;
                    self.decrypt_next_message(next_message_size)?;
                }
            } else {
                let needed_padding = next_message_size.saturating_sub(self.encrypted_buffer.len());
                let cursor = self.encrypted_buffer.len();
                self.encrypted_buffer
                    .extend(iter::repeat(0).take(needed_padding));
                self.inner
                    .read_exact(&mut self.encrypted_buffer[cursor..])?;

                self.decrypt_next_message(next_message_size)?;
            }
        }

        let bytes_to_consume = buf.len().min(self.decrypted_buffer.len());
        buf[..bytes_to_consume].copy_from_slice(&self.decrypted_buffer[..bytes_to_consume]);
        self.decrypted_buffer.drain(..bytes_to_consume);
        Ok(bytes_to_consume)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MiB;
    use aead::{generic_array::GenericArray, KeyInit};

    #[test]
    fn roundtrip() {
        let plaintext: Vec<_> = iter::repeat_with(rand::random::<u8>)
            .take(64 * MiB + 5)
            .collect();

        let mut encrypted = Vec::new();

        let key = Aes256GcmSiv::generate_key(&mut rand::thread_rng());
        let nonce: Nonce<Aes256GcmSiv, StreamLE31<Aes256GcmSiv>> =
            GenericArray::from(rand::random::<[u8; 8]>());
        let mut encryptor = WriteEncrypted::new(&mut encrypted, &key, &nonce);
        for chunk in plaintext.chunks(256) {
            encryptor.write_all(chunk).unwrap();
        }
        encryptor.finish().unwrap();

        let mut decryptor = ReadEncrypted::new(&encrypted[..], &key, &nonce);
        let mut decoded_plaintext = Vec::new();
        decryptor.read_to_end(&mut decoded_plaintext).unwrap();

        assert_eq!(decoded_plaintext, plaintext);
    }
}
