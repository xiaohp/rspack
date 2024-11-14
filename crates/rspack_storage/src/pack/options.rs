#[derive(Debug, Clone)]
pub struct PackStorageOptions {
  pub buckets: usize,
  pub max_pack_size: usize,
  pub expires: u64,
}


