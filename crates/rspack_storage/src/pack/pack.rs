use std::{
  fs::{remove_file, File},
  hash::{Hash, Hasher},
  io::{BufRead, BufReader, BufWriter, Write},
  os::unix::fs::MetadataExt,
  path::PathBuf,
};

use rspack_error::{error, miette::Error, Result};
use rustc_hash::FxHasher;

pub type PackKeys = Vec<Vec<u8>>;

#[derive(Debug, Default)]
pub enum PackKeysState {
  #[default]
  Pending,
  Failed(Error),
  Value(PackKeys),
}

impl PackKeysState {
  pub fn expect_value(&self) -> Result<&PackKeys> {
    match self {
      PackKeysState::Value(v) => Ok(v),
      PackKeysState::Failed(e) => Err(error!("{}", e)),
      PackKeysState::Pending => Err(error!("pack key is not ready")),
    }
  }
  pub fn expect_value_mut(&mut self) -> Result<&mut PackKeys> {
    match self {
      PackKeysState::Value(v) => Ok(v),
      PackKeysState::Failed(e) => Err(error!("{}", e)),
      PackKeysState::Pending => Err(error!("pack key is not ready")),
    }
  }
}

pub type PackContents = Vec<Vec<u8>>;

#[derive(Debug, Default)]
pub enum PackContentsState {
  #[default]
  Pending,
  Failed(Error),
  Value(PackContents),
}

impl PackContentsState {
  pub fn expect_value(&self) -> Result<&PackContents> {
    match self {
      PackContentsState::Value(v) => Ok(v),
      PackContentsState::Failed(e) => Err(error!("{}", e)),
      PackContentsState::Pending => Err(error!("pack content is not ready")),
    }
  }
  pub fn expect_value_mut(&mut self) -> Result<&mut PackContents> {
    match self {
      PackContentsState::Value(v) => Ok(v),
      PackContentsState::Failed(e) => Err(error!("{}", e)),
      PackContentsState::Pending => Err(error!("pack content is not ready")),
    }
  }
}

#[derive(Debug)]
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

  pub fn write(&self) -> Result<()> {
    let mut writer = BufWriter::new(File::create(&self.path).expect("should create file"));
    if self.path.exists() {
      remove_file(&self.path).map_err(|e| error!("failed to remove old pack file: {}", e))?;
    }
    let PackKeysState::Value(keys) = &self.keys else {
      return Err(error!("pack keys is not ready"));
    };

    let PackContentsState::Value(contents) = &self.contents else {
      return Err(error!("pack contents is not ready"));
    };

    if keys.len() != contents.len() {
      return Err(error!("pack keys and contents length not match"));
    }

    let key_meta_line = keys
      .iter()
      .map(|key| key.len().to_string())
      .collect::<Vec<_>>()
      .join(" ");
    writer
      .write_fmt(format_args!("{}\n", key_meta_line))
      .map_err(|e| error!("write pack failed: {}", e))?;

    for key in keys {
      writer
        .write(key)
        .map_err(|e| error!("write pack key failed: {}", e))?;
    }
    writer.write(b"\n");

    for value in contents {
      writer
        .write(value)
        .map_err(|e| error!("write pack value failed: {}", e))?;
      writer.write(b"\n");
    }

    Ok(())
  }

  pub fn remove_file(&self) -> Result<()> {
    if self.path.exists() {
      remove_file(&self.path).map_err(|e| error!("failed to remove pack file: {}", e))
    } else {
      Ok(())
    }
  }

  pub fn read_keys(path: &PathBuf) -> Result<PackKeys> {
    if !path.exists() {
      return Err(error!(
        "cache pack file `{}` does not exists",
        path.display()
      ));
    }

    let file = File::open(&path).map_err(|e| error!("open pack file failed: {}", e))?;
    let mut lines = BufReader::new(file).lines();

    let Some(Ok(next_line)) = lines.next() else {
      return Err(error!("failed to read pack key meta"));
    };
    let key_meta_list: Vec<usize> = next_line
      .split(" ")
      .map(|item| item.parse::<usize>().expect("should have meta info"))
      .collect();

    let Some(Ok(next_line)) = lines.next() else {
      return Err(error!("failed to read pack keys"));
    };
    let mut keys = vec![];
    let mut last = 0;
    let bytes = next_line.as_bytes();
    for key_len in key_meta_list {
      let start = last;
      let end = last + key_len;
      let key = &bytes[start..end].to_vec();
      last = end;
      keys.push(key.to_owned());
    }
    Ok(keys)
  }

  pub fn read_contents(path: &PathBuf, keys: &PackKeys) -> Result<PackContents> {
    if !path.exists() {
      return Err(error!(
        "cache pack file `{}` does not exists",
        path.display()
      ));
    }
    let file = File::open(&path).map_err(|e| error!("open pack file failed: {}", e))?;
    let mut lines = BufReader::new(file).lines().skip(2);

    let mut res = vec![];
    for _ in keys {
      let Some(Ok(next_line)) = lines.next() else {
        return Err(error!("pack keys not match their contents"));
      };
      res.push(next_line.as_bytes().to_owned());
    }

    Ok(res)
  }

  pub fn validate(&self, path: &PathBuf, hash: &String) -> Result<bool> {
    let key_hash = Self::get_keys_hash(&Self::new(path.to_owned()))?;
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

  pub fn get_keys_hash(&self) -> Result<String> {
    let PackKeysState::Value(keys) = &self.keys else {
      return Err(error!("pack keys is not ready"));
    };
    let mut hasher = FxHasher::default();
    for k in keys {
      hasher.write(k);
    }
    hasher.write_usize(keys.len());
    Ok(format!("{:016x}", hasher.finish()))
  }
}
