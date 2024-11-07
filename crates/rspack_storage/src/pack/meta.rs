use std::{
  fs::{remove_file, File},
  io::{BufRead, BufReader, BufWriter, Write},
  path::PathBuf,
  time::{SystemTime, UNIX_EPOCH},
};

use itertools::Itertools;
use rspack_error::{error, Result};

use super::PackStorageOptions;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PackFileMeta {
  pub hash: String,
  pub name: String,
}

#[derive(Debug, Default)]
pub struct ScopeMeta {
  pub buckets: usize,
  pub max_pack_size: usize,
  pub last_modified: u64,
  pub packs: Vec<Vec<PackFileMeta>>,
}

impl ScopeMeta {
  pub fn new(options: &PackStorageOptions) -> Self {
    let mut empty_packs: Vec<_> = vec![];
    for _ in 0..options.buckets {
      empty_packs.push(vec![]);
    }
    Self {
      buckets: options.buckets,
      max_pack_size: options.max_pack_size,
      last_modified: SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("should get current time")
        .as_secs(),
      packs: empty_packs,
    }
  }

  pub fn write(&self, dir: &PathBuf) -> Result<()> {
    let file_path = dir.join("cache_meta");
    if file_path.exists() {
      remove_file(&file_path).map_err(|e| error!("failed to remove old meta file: {}", e))?;
    }

    let mut writer = BufWriter::new(File::create(file_path).expect("should create file"));
    writer
      .write_fmt(format_args!(
        "{} {} {}\n",
        self.buckets, self.max_pack_size, self.last_modified
      ))
      .map_err(|e| error!("write meta failed: {}", e))?;
    for bucket_id in 0..self.buckets {
      let line = self
        .packs
        .get(bucket_id)
        .map(|packs| {
          packs
            .iter()
            .map(|meta| format!("{},{}", meta.name, meta.hash))
            .join(" ")
        })
        .unwrap_or_default();
      writer
        .write_fmt(format_args!("{}\n", line))
        .map_err(|e| error!("write meta failed: {}", e))?;
    }

    Ok(())
  }

  pub fn read(dir: &PathBuf) -> Result<Self> {
    let meta_file_path = dir.join("cache_meta");
    if !meta_file_path.exists() {
      return Err(error!("cache meta does not exists"));
    }
    let meta_file =
      File::open(meta_file_path).map_err(|e| error!("open meta file failed: {}", e))?;
    let mut reader = BufReader::new(meta_file);
    let mut next_line = String::new();
    reader
      .read_line(&mut next_line)
      .map_err(|e| error!("read meta file failed: {}", e))?;

    let meta_options = next_line
      .split(" ")
      .map(|item| {
        item
          .parse::<u64>()
          .map_err(|e| error!("parse meta file failed: {}", e))
      })
      .collect::<Result<Vec<u64>>>()?;

    if meta_options.len() < 3 {
      return Err(error!("meta broken"));
    }

    let buckets = meta_options[0] as usize;
    let max_pack_size = meta_options[1] as usize;
    let last_modified = meta_options[2];

    let mut index = 0;
    let mut packs = vec![];
    while index < buckets {
      let bytes = reader
        .read_line(&mut next_line)
        .map_err(|e| error!("read meta file failed: {}", e))?;
      if bytes == 0 {
        break;
      }
      packs.push(
        next_line
          .split(" ")
          .map(|x| x.split(",").collect::<Vec<_>>())
          .map(|info| {
            if info.len() < 2 {
              Err(error!("parse pack file info failed"))
            } else {
              Ok(PackFileMeta {
                name: info[0].to_owned(),
                hash: info[1].to_owned(),
              })
            }
          })
          .collect::<Result<Vec<PackFileMeta>>>()?,
      );
      index += 1;
    }

    if packs.len() < buckets {
      return Err(error!("parse meta buckets failed"));
    }

    Ok(ScopeMeta {
      buckets,
      max_pack_size,
      last_modified,
      packs,
    })
  }
}
