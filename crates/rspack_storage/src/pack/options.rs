use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct PackStorageOptions {
  pub location: PathBuf,
  pub buckets: usize,
  pub max_pack_size: usize,
  pub expires: u64,
}
