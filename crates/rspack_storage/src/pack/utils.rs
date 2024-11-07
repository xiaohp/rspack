use std::{fs::File, hash::Hasher, os::unix::fs::MetadataExt, path::PathBuf};

use rspack_error::{error, Result};
use rustc_hash::FxHasher;

use super::PackKeys;

pub fn get_keys_hash(keys: &PackKeys) -> String {
  let mut hasher = FxHasher::default();
  for k in keys {
    hasher.write(k);
  }
  hasher.write_usize(keys.len());
  format!("{:016x}", hasher.finish())
}

pub fn validate_pack(path: PathBuf, keys: PackKeys, hash: String) -> Result<bool> {
  let key_hash = get_keys_hash(&keys);
  let base_name = path
    .file_stem()
    .unwrap_or_default()
    .to_string_lossy()
    .to_string();

  if key_hash != base_name {
    return Ok(false);
  }

  let mut hasher = FxHasher::default();
  hasher.write(key_hash.as_bytes());

  let file = File::open(&path).map_err(|e| error!("open pack file failed: {}", e))?;
  let meta_data = file
    .metadata()
    .map_err(|e| error!("open pack file failed: {}", e))?;

  hasher.write_u64(meta_data.size());

  let mtime = meta_data.mtime_nsec();
  hasher.write_i64(mtime);

  Ok(*hash == format!("{:016x}", hasher.finish()))
}
