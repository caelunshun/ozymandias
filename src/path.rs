use serde::{Deserialize, Serialize};
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};

/// A simplified file path relative to a directory root.
///
/// Normalized to contain no "..", ".", trailing slashes, etc.
/// Considers symlinks to be inlined into the parent directory:
/// e.g. "symlink/../a/b" resolves to "a/b" and not to the parent
/// directory of the symlink target (which would be the case with `fs::canonicalize`).
#[derive(Default, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SimplifiedPath {
    components: Vec<OsString>,
}

impl SimplifiedPath {
    /// Push a new single component to the path.
    ///
    ///  `component` should not contain slashes or consist of only periods.
    pub fn push(&mut self, component: impl AsRef<OsStr>) {
        let component = component.as_ref().to_owned();
        self.components.push(component);
    }

    /// Returns an instantiated `PathBuf` from the given root.
    pub fn instantiate_with_root(&self, root: &Path) -> PathBuf {
        let mut buf = root.to_path_buf();
        for component in &self.components {
            buf.push(component);
        }
        buf
    }
}
