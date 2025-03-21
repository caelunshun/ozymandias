use anyhow::Context;
use fs_err as fs;
use ozymandias::{
    backup,
    medium::{
        compressing::{CompressingMedium, CompressionType},
        encrypting::EncryptingMedium,
        local::LocalMedium,
        Medium,
    },
    model::Version,
    restore,
};
use std::iter;
use tempfile::tempdir;

#[test]
fn backup_and_restore() -> anyhow::Result<()> {
    let src_dir = tempdir()?;
    let backup_dir = tempdir()?;
    let restore_dir = tempdir()?;

    fs::remove_dir(&restore_dir)?;

    let file_a_contents = "foobarbaz";
    let file_b_contents = "ozymandias".repeat(1024 * 64);
    let file_c_contents: Vec<u8> = iter::repeat_with(rand::random::<u8>)
        .take(1024 * 1024 * 256)
        .collect();

    fs::write(src_dir.path().join("a"), file_a_contents)?;
    fs::write(src_dir.path().join("b"), &file_b_contents)?;
    fs::write(src_dir.path().join("c"), &file_c_contents)?;

    let medium = LocalMedium::new(backup_dir.path(), "the_backup")?;
    let medium = EncryptingMedium::with_password(medium, Some("ozymandias123"));
    let medium = CompressingMedium::new(
        medium,
        CompressionType::Zstd {
            compression_level: zstd::DEFAULT_COMPRESSION_LEVEL,
        },
    );

    backup::run(backup::Config {
        source_dir: src_dir.path().to_path_buf(),
        medium: &medium,
    })?;
    let version = medium.load_version(0)?.context("version not created")?;
    let version = Version::decode(&version[..])?;
    restore::run(&medium, &version, restore_dir.path())?;

    let a = fs::read(restore_dir.path().join("a"))?;
    assert_eq!(a.as_slice(), file_a_contents.as_bytes());
    let b = fs::read(restore_dir.path().join("b"))?;
    assert_eq!(b.as_slice(), file_b_contents.as_bytes());
    let c = fs::read(restore_dir.path().join("c"))?;
    assert_eq!(c.as_slice(), file_c_contents.as_slice());

    Ok(())
}
