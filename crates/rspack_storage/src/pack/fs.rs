use std::{
  fs::{remove_dir_all, File},
  hash::Hasher,
  io::{BufRead, BufReader, BufWriter, Read, Write},
  os::unix::fs::MetadataExt,
  path::PathBuf,
  sync::Arc,
};

use futures::{executor::block_on, future::join_all};
use itertools::Itertools;
use rspack_error::{error, Result};
use rustc_hash::FxHasher;

use super::{PackContents, PackFileMeta, PackKeys, ScopeMeta};

#[derive(Debug, Clone)]
pub struct PackStorageFs {
  // TODO: input/output file system
  pub temp_root: PathBuf,
  pub root: PathBuf,
}

impl PackStorageFs {
  pub fn new(root: PathBuf, temp_root: PathBuf) -> Self {
    Self { root, temp_root }
  }

  pub fn ensure_root(&self) -> Result<()> {
    self.ensure_dir(&self.root)?;
    self.ensure_dir(&self.temp_root)?;
    Ok(())
  }

  pub fn redirect_to_temp(&self, path: &PathBuf) -> Result<PathBuf> {
    let relative_path = path
      .strip_prefix(&self.root)
      .map_err(|e| error!("failed to get relative path: {}", e))?;
    Ok(self.temp_root.join(relative_path))
  }

  pub fn remove_dir(&self, path: &PathBuf) -> Result<()> {
    if path.exists() {
      remove_dir_all(path).map_err(|e| error!("{}", e))
    } else {
      Ok(())
    }
  }

  pub fn ensure_dir(&self, path: &PathBuf) -> Result<()> {
    std::fs::create_dir_all(path).map_err(|e| error!("{}", e))
  }

  pub fn write_pack(&self, path: &PathBuf, keys: &PackKeys, contents: &PackContents) -> Result<()> {
    let path = self.redirect_to_temp(path)?;
    self.ensure_dir(&PathBuf::from(path.parent().expect("should have parent")))?;

    let mut writer = BufWriter::new(File::create(path).expect("should create file"));

    // key meta line
    let key_meta_line = keys
      .iter()
      .map(|key| key.len().to_string())
      .collect::<Vec<_>>()
      .join(" ");

    writer
      .write_fmt(format_args!("{}\n", key_meta_line))
      .map_err(|e| error!("write pack key meta failed: {}", e))?;

    // content meta line
    let content_meta_line = contents
      .iter()
      .map(|content| content.len().to_string())
      .collect::<Vec<_>>()
      .join(" ");
    writer
      .write_fmt(format_args!("{}\n", content_meta_line))
      .map_err(|e| error!("write pack content meta failed: {}", e))?;

    for key in keys {
      writer
        .write(key)
        .map_err(|e| error!("write pack key failed: {}", e))?;
    }

    for value in contents {
      writer
        .write(value)
        .map_err(|e| error!("write pack value failed: {}", e))?;
    }

    writer
      .flush()
      .map_err(|e| error!("write pack value failed: {}", e))?;

    Ok(())
  }

  pub fn read_pack_file_hash(&self, path: &PathBuf) -> Result<String> {
    if !path.exists() {
      println!("read pack file: {:?}", path);
      return Err(error!(
        "cache pack file `{}` does not exists",
        path.display()
      ));
    }

    let mut hasher = FxHasher::default();

    let file = File::open(&path).map_err(|e| error!("open pack file failed: {}", e))?;
    let meta_data = file
      .metadata()
      .map_err(|e| error!("open pack file failed: {}", e))?;

    hasher.write_u64(meta_data.size());

    let mtime = meta_data.mtime_nsec();
    hasher.write_i64(mtime);

    Ok(format!("{:016x}", hasher.finish()))
  }

  pub fn read_pack_keys(&self, path: &PathBuf) -> Result<Option<PackKeys>> {
    if !path.exists() {
      return Ok(None);
    }

    let file = File::open(&path).map_err(|e| error!("open pack file failed: {}", e))?;
    let mut reader = BufReader::new(file);
    let mut next_line = String::new();
    reader
      .read_line(&mut next_line)
      .expect("read pack key meta faield");

    next_line.pop();

    let key_meta_list: Vec<usize> = next_line
      .split(" ")
      .map(|item| item.parse::<usize>().expect("should have meta info"))
      .collect();

    // skip content meta line
    reader
      .read_line(&mut next_line)
      .expect("read pack key meta faield");

    let mut keys = vec![];
    for key_len in key_meta_list {
      let mut key = vec![0u8; key_len];
      reader.read_exact(&mut key).expect("should have key");
      keys.push(Arc::new(key));
    }
    Ok(Some(keys))
  }

  pub fn read_pack_contents(
    &self,
    path: &PathBuf,
    keys: &PackKeys,
  ) -> Result<Option<PackContents>> {
    if !path.exists() {
      return Ok(None);
    }

    let total_key_size = keys.iter().fold(0_i64, |acc, key| acc + key.len() as i64);
    let file = File::open(&path).map_err(|e| error!("open pack file failed: {}", e))?;
    let mut reader = BufReader::new(file);
    let mut next_line = String::new();

    // skip key meta line
    reader
      .read_line(&mut next_line)
      .expect("read pack key meta faield");

    next_line.clear();
    // read content meta line
    reader
      .read_line(&mut next_line)
      .expect("read pack content meta faield");
    next_line.pop();

    let content_meta_list: Vec<usize> = next_line
      .split(" ")
      .map(|item| item.parse::<usize>().expect("should have meta info"))
      .collect();

    // skip keys
    reader
      .seek_relative(total_key_size)
      .expect("should skip keys");

    // read contents
    let mut res = vec![];
    for content_len in content_meta_list {
      let mut content = vec![0u8; content_len];
      reader.read_exact(&mut content).expect("should have key");
      res.push(Arc::new(content));
    }

    Ok(Some(res))
  }

  pub fn write_scope_meta(&self, meta: &ScopeMeta) -> Result<()> {
    let path = self.redirect_to_temp(&meta.path)?;
    self.ensure_dir(&PathBuf::from(path.parent().expect("should have parent")))?;

    let mut writer = BufWriter::new(File::create(path).expect("should create file"));

    writer
      .write_fmt(format_args!(
        "{} {} {}\n",
        meta.buckets, meta.max_pack_size, meta.last_modified
      ))
      .map_err(|e| error!("write meta failed: {}", e))?;

    for bucket_id in 0..meta.buckets {
      let line = meta
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

  pub fn read_scope_meta(&self, path: PathBuf) -> Result<Option<ScopeMeta>> {
    if !path.exists() {
      return Ok(None);
    }

    let meta_file = File::open(&path).map_err(|e| error!("open meta file failed: {}", e))?;
    let mut lines = BufReader::new(meta_file).lines();

    let meta_line = match lines.next() {
      Some(line) => line.map_err(|e| error!("read meta file failed: {}", e))?,
      None => return Err(error!("read meta file failed: no meta line")),
    };

    let meta_options = meta_line
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

    let mut bucket_id = 0;
    let mut packs = vec![];
    while bucket_id < buckets {
      let bucket_line = match lines.next() {
        Some(line) => line.map_err(|e| error!("read meta file failed: {}", e))?,
        None => {
          return Err(error!(
            "read meta file failed: no bucket {} line",
            bucket_id
          ))
        }
      };
      packs.push(
        bucket_line
          .split(" ")
          .filter(|x| x.contains(","))
          .map(|x| x.split(",").collect::<Vec<_>>())
          .map(|info| {
            if info.len() < 2 {
              Err(error!("parse pack file info failed"))
            } else {
              Ok(Arc::new(PackFileMeta {
                name: info[0].to_owned(),
                hash: info[1].to_owned(),
              }))
            }
          })
          .collect::<Result<Vec<Arc<PackFileMeta>>>>()?,
      );
      bucket_id += 1;
    }

    if packs.len() < buckets {
      return Err(error!("parse meta buckets failed"));
    }

    Ok(Some(ScopeMeta {
      path,
      buckets,
      max_pack_size,
      last_modified,
      packs,
    }))
  }

  pub fn move_temporary(
    &self,
    writed_files: &Vec<PathBuf>,
    removed_files: &Vec<PathBuf>,
  ) -> Result<()> {
    let mut tasks = vec![];
    println!("writed files: {:?}", writed_files);
    println!("removed files: {:?}", removed_files);

    async fn handle_file(file: &PathBuf, fs: &PackStorageFs, is_removed: bool) -> Result<()> {
      if is_removed {
        if file.exists() {
          return tokio::fs::remove_file(file)
            .await
            .map_err(|e| error!("{}", e));
        }
      } else {
        let temp_file = fs.redirect_to_temp(file)?;
        if temp_file.exists() {
          tokio::fs::create_dir_all(file.parent().expect("should have parent"))
            .await
            .map_err(|e: std::io::Error| error!("{}", e))?;

          return tokio::fs::rename(&temp_file, file)
            .await
            .map_err(|e| error!("{}", e));
        }
      }
      Ok(())
    }

    for file in writed_files {
      self.ensure_dir(&PathBuf::from(file.parent().expect("should have parent")))?;
      let temp_file = self.redirect_to_temp(file)?;
      if temp_file.exists() {
        std::fs::rename(temp_file, file).map_err(|e| error!("{}", e))?;
      }
    }

    for file in removed_files {
      tasks.push(handle_file(file, &self, true));
    }

    println!("start handle files");
    block_on(join_all(tasks))
      .into_iter()
      .collect::<Result<Vec<()>>>()?;

    println!("end handle files");
    Ok(())
  }

  pub fn clean_cache(&self) -> Result<()> {
    Ok(())
  }

  pub fn clean_temporary(&self) -> Result<()> {
    self.remove_dir(&self.temp_root)
  }
}
