/*use fs_err as fs;
use ozymandias::medium::fs::FilesystemMedium;
use ozymandias::{backup, restore};
use std::iter;
use tempfile::tempdir;

#[test]
fn backup_and_restore() -> anyhow::Result<()> {
    tracing_subscriber::fmt::try_init().ok();

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

    let mut medium = FilesystemMedium::new(backup_dir.path())?;

    backup::Driver::new(&mut medium, &src_dir).run()?;
    restore::Driver::new(&mut medium, &restore_dir)?.run(0)?;

    let a = fs::read(restore_dir.path().join("a"))?;
    assert_eq!(a.as_slice(), file_a_contents.as_bytes());
    let b = fs::read(restore_dir.path().join("b"))?;
    assert_eq!(b.as_slice(), file_b_contents.as_bytes());
    let c = fs::read(restore_dir.path().join("c"))?;
    assert_eq!(c.as_slice(), file_c_contents.as_slice());

    Ok(())
}*/
