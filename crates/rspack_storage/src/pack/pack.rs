use std::{hash::Hasher, path::PathBuf, sync::Arc};

use rspack_error::{error, Result};
use rustc_hash::FxHasher;

use super::{PackFileMeta, PackStorageFs, PackStorageOptions};

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

pub fn fill_packs(
  items: &mut Vec<(Arc<Vec<u8>>, Arc<Vec<u8>>)>,
  dir: &PathBuf,
  options: &PackStorageOptions,
) -> Vec<(PackFileMeta, Pack)> {
  println!("new packs items: {:?}", items.len());
  items.sort_unstable_by(|a, b| a.1.len().cmp(&b.1.len()));

  let mut new_packs = vec![];

  fn create_pack(dir: &PathBuf, keys: PackKeys, contents: PackContents) -> (PackFileMeta, Pack) {
    let file_name = get_pack_name(&keys);
    let mut new_pack = Pack::new(dir.join(file_name.clone()));
    new_pack.keys = PackKeysState::Value(keys);
    new_pack.contents = PackContentsState::Value(contents);
    (
      PackFileMeta {
        name: file_name,
        hash: Default::default(),
      },
      new_pack,
    )
  }

  loop {
    if items.len() == 0 {
      break;
    }
    // handle big single cache
    if items.last().expect("should have first item").1.len() as f64
      > options.max_pack_size as f64 * 0.5_f64
    {
      let (key, value) = items.pop().expect("shoud have first item");
      new_packs.push(create_pack(dir, vec![key], vec![value]));
    } else {
      break;
    }
  }

  loop {
    let mut batch_keys: PackKeys = vec![];
    let mut batch_contents: PackContents = vec![];
    let mut batch_size = 0_usize;

    loop {
      if items.len() == 0 {
        break;
      }

      if batch_size + items.last().expect("should have first item").1.len() > options.max_pack_size
      {
        break;
      }

      let (key, value) = items.pop().expect("shoud have first item");
      batch_size += value.len();
      batch_keys.push(key);
      batch_contents.push(value);
    }

    if !batch_keys.is_empty() {
      new_packs.push(create_pack(dir, batch_keys, batch_contents));
    }

    if items.len() == 0 {
      break;
    }
  }

  println!("new packs: {:?}", new_packs.len());
  new_packs
}
