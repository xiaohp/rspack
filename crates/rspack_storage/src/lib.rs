// mod fs;
mod pack;

use std::sync::Arc;

// pub use fs::FsStorage;
pub use pack::{PackStorage, PackStorageOptions};
use rspack_error::Result;

pub trait Storage: std::fmt::Debug + Sync + Send {
  fn get_all(&self, scope: &'static str) -> Result<Vec<(Arc<Vec<u8>>, Arc<Vec<u8>>)>>;
  //  fn get(&self, scope: &str, key: &[u8]) -> Option<Vec<u8>>;
  // using immutable reference to support concurrency
  fn set(&self, scope: &'static str, key: Vec<u8>, value: Vec<u8>);
  fn remove(&self, scope: &'static str, key: &[u8]);
  fn idle(&self) -> Result<()>;
}

pub type ArcStorage = Arc<dyn Storage>;
