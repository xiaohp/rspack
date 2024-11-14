use std::{hash::Hasher, path::PathBuf, sync::Arc};

use rspack_error::{error, Result};
use rustc_hash::FxHasher;

use super::PackStorageFs;

pub type PackKeys = Vec<Arc<Vec<u8>>>;

#[derive(Debug, Default, Clone)]
pub enum PackKeysState {
  #[default]
  Pending,
  Value(PackKeys),
}

impl PackKeysState {
  pub fn expect_value(&self) -> &PackKeys {
    match self {
      PackKeysState::Value(v) => v,
      PackKeysState::Pending => panic!("pack key is not ready"),
    }
  }
}

pub type PackContents = Vec<Arc<Vec<u8>>>;

#[derive(Debug, Default, Clone)]
pub enum PackContentsState {
  #[default]
  Pending,
  Value(PackContents),
}

impl PackContentsState {
  pub fn expect_value(&self) -> &PackContents {
    match self {
      PackContentsState::Value(v) => v,
      PackContentsState::Pending => panic!("pack content is not ready"),
    }
  }
}

#[derive(Debug, Clone)]
pub struct Pack {
  pub path: PathBuf,
  pub keys: PackKeysState,
  pub contents: PackContentsState,
}

impl Pack {
  pub fn new(path: PathBuf) -> Self {
    Self {
      path,
      keys: Default::default(),
      contents: Default::default(),
    }
  }

  pub fn write(&self, fs: &PackStorageFs) -> Result<()> {
    let keys = self.keys.expect_value();
    let contents = self.contents.expect_value();
    if keys.len() != contents.len() {
      return Err(error!("pack keys and contents length not match"));
    }

    fs.write_pack(&self.path, keys, contents)?;

    Ok(())
  }

  pub fn loaded(&self) -> bool {
    matches!(self.keys, PackKeysState::Value(_))
      && matches!(self.contents, PackContentsState::Value(_))
  }
}

pub fn get_pack_name(keys: &PackKeys) -> String {
  let mut hasher = FxHasher::default();
  for k in keys {
    hasher.write(k);
  }
  hasher.write_usize(keys.len());

  format!("{:016x}", hasher.finish())
}

pub fn get_pack_hash(path: &PathBuf, keys: &PackKeys, fs: &PackStorageFs) -> Result<String> {
  let mut hasher = FxHasher::default();
  let file_name = get_pack_name(keys);
  hasher.write(file_name.as_bytes());

  let file_hash = fs.read_pack_file_hash(&path)?;
  hasher.write(file_hash.as_bytes());

  Ok(format!("{:016x}", hasher.finish()))
}

pub fn validate_pack(
  hash: &str,
  path: &PathBuf,
  keys: &PackKeys,
  fs: &PackStorageFs,
) -> Result<bool> {
  let pack_hash = get_pack_hash(path, keys, fs)?;
  Ok(*hash == pack_hash)
}
