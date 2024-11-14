use std::{
  hash::Hasher,
  path::PathBuf,
  sync::Arc,
  time::{SystemTime, UNIX_EPOCH},
};

use futures::{executor::block_on, future::join_all, TryFutureExt};
use itertools::Itertools;
use rayon::iter::{IntoParallelIterator, IntoParallelRefMutIterator, ParallelIterator};
use rspack_error::{error, Result};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet, FxHasher};
use tokio::runtime::Handle;

use super::{
  get_pack_hash, get_pack_name, validate_pack, Pack, PackContents, PackContentsState, PackFileMeta,
  PackStorageFs, PackStorageOptions, ScopeMeta,
};
use crate::pack::{PackKeys, PackKeysState};

#[derive(Debug)]
pub enum ScopeValidateResult {
  Valid,
  Invalid(String),
}

#[derive(Debug, Default, Clone)]
pub enum ScopeMetaState {
  #[default]
  Pending,
  Value(ScopeMeta),
}

impl ScopeMetaState {
  pub fn expect_value(&self) -> &ScopeMeta {
    match self {
      ScopeMetaState::Value(v) => v,
      ScopeMetaState::Pending => panic!("should have scope meta"),
    }
  }
  pub fn take_value(&mut self) -> Option<ScopeMeta> {
    match self {
      ScopeMetaState::Value(v) => Some(std::mem::take(&mut *v)),
      _ => None,
    }
  }
}

type ScopePacks = Vec<Vec<Pack>>;

#[derive(Debug, Default, Clone)]
pub enum ScopePacksState {
  #[default]
  Pending,
  Value(ScopePacks),
}

impl ScopePacksState {
  pub fn expect_value(&self) -> &ScopePacks {
    match self {
      ScopePacksState::Value(v) => v,
      ScopePacksState::Pending => panic!("scope meta is not ready"),
    }
  }
  pub fn expect_value_mut(&mut self) -> &mut ScopePacks {
    match self {
      ScopePacksState::Value(v) => v,
      ScopePacksState::Pending => panic!("scope meta is not ready"),
    }
  }
  pub fn take_value(&mut self) -> Option<ScopePacks> {
    match self {
      ScopePacksState::Value(v) => Some(std::mem::take(&mut *v)),
      _ => None,
    }
  }
}

#[derive(Debug, Clone)]
pub struct PackScope {
  pub name: &'static str,
  pub path: PathBuf,
  pub options: PackStorageOptions,
  pub meta: ScopeMetaState,
  pub packs: ScopePacksState,
}

impl PackScope {
  pub fn new(name: &'static str, dir: &PathBuf, options: PackStorageOptions) -> Self {
    Self {
      name,
      path: dir.join(name),
      options,
      meta: ScopeMetaState::Pending,
      packs: ScopePacksState::Pending,
    }
  }

  pub fn empty(name: &'static str, dir: &PathBuf, options: PackStorageOptions) -> Self {
    let scope_path = dir.join(name);
    let meta = ScopeMeta::new(&scope_path, &options);
    let packs = vec![vec![]; options.buckets];
    Self {
      name,
      path: scope_path,
      options,
      meta: ScopeMetaState::Value(meta),
      packs: ScopePacksState::Value(packs),
    }
  }

  pub fn loaded(&self) -> bool {
    matches!(self.meta, ScopeMetaState::Value(_))
      && matches!(self.packs, ScopePacksState::Value(_))
      && self
        .packs
        .expect_value()
        .iter()
        .flatten()
        .all(|pack| pack.loaded())
  }

  pub fn get_contents(&mut self, fs: &PackStorageFs) -> Result<Vec<(Arc<Vec<u8>>, Arc<Vec<u8>>)>> {
    self.ensure_pack_contents(fs)?;

    let packs = self.packs.expect_value();
    let contents = packs
      .iter()
      .flatten()
      .filter_map(|pack| {
        if let (PackKeysState::Value(keys), PackContentsState::Value(contents)) =
          (&pack.keys, &pack.contents)
        {
          if keys.len() == contents.len() {
            return Some(
              keys
                .iter()
                .enumerate()
                .map(|(index, key)| (key.clone(), contents[index].clone()))
                .collect_vec(),
            );
          }
        }
        None
      })
      .flatten()
      .collect_vec();

    Ok(contents)
  }

  pub fn validate(
    &mut self,
    options: &PackStorageOptions,
    fs: &PackStorageFs,
  ) -> Result<ScopeValidateResult> {
    self.ensure_meta(fs)?;

    // validate meta
    let meta = self.meta.expect_value();
    if meta.buckets != options.buckets || meta.max_pack_size != options.max_pack_size {
      return Ok(ScopeValidateResult::Invalid(
        "scope options changed".to_string(),
      ));
    }

    let current_time = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .expect("get current time failed")
      .as_secs();

    if current_time - meta.last_modified > options.expires {
      return Ok(ScopeValidateResult::Invalid("scope expired".to_string()));
    }

    // validate packs
    let validate = self.validate_packs(fs)?;
    if validate {
      Ok(ScopeValidateResult::Valid)
    } else {
      Ok(ScopeValidateResult::Invalid(
        "scope cache files changed".to_string(),
      ))
    }
  }

  fn validate_packs(&mut self, fs: &PackStorageFs) -> Result<bool> {
    self.ensure_pack_keys(fs)?;

    let packs = self.get_pack_meta_pairs()?;

    let tasks = packs
      .iter()
      .filter(|(_, _, _, pack)| matches!(pack.keys, PackKeysState::Value(_)))
      .map(|(bucket_id, _, pack_meta, pack)| {
        let keys = pack.keys.expect_value();
        let pack_path = self.path.join(bucket_id.to_string()).join(&pack_meta.name);
        let pack_hash = pack_meta.hash.to_owned();
        let pack_keys = keys.to_owned();
        let fs = fs.to_owned();
        // TODO: collect failed packs
        tokio::spawn(async move {
          match validate_pack(&pack_hash, &pack_path, &pack_keys, &fs) {
            Ok(res) => res,
            Err(_) => false,
          }
        })
        .map_err(|e| error!("{}", e))
      });

    let pack_validates = block_on(join_all(tasks))
      .into_iter()
      .collect::<Result<Vec<bool>>>()?;

    Ok(pack_validates.iter().all(|v| *v))
  }

  fn ensure_pack_keys(&mut self, fs: &PackStorageFs) -> Result<()> {
    self.ensure_packs(fs)?;

    let packs_pairs = self.get_pack_meta_pairs()?;

    async fn read_pack_keys(
      bid: usize,
      pid: usize,
      path: &PathBuf,
      fs: &PackStorageFs,
    ) -> Result<(usize, usize, PackKeys)> {
      match fs.read_pack_keys(&path)? {
        Some(v) => Ok((bid, pid, v)),
        None => Ok((bid, pid, vec![])),
      }
    }

    let tasks = packs_pairs
      .into_iter()
      .filter(|(_, _, _, pack)| matches!(pack.keys, PackKeysState::Pending))
      .map(|(bucket_id, pack_pos, _, pack)| {
        let pack_path = pack.path.to_owned();
        let fs = fs.to_owned();
        tokio::spawn(async move { read_pack_keys(bucket_id, pack_pos, &pack_path, &fs).await })
          .map_err(|e| error!("{}", e))
      });

    let task_results = block_on(join_all(tasks))
      .into_iter()
      .collect::<Result<Vec<Result<(usize, usize, PackKeys)>>>>()?;

    let packs = self.packs.expect_value_mut();

    for task_result in task_results {
      let (bucket_id, pack_pos, item) = task_result?;
      if let Some(pack) = packs
        .get_mut(bucket_id)
        .and_then(|packs| packs.get_mut(pack_pos))
      {
        pack.keys = PackKeysState::Value(item);
      }
    }

    Ok(())
  }

  fn ensure_pack_contents(&mut self, fs: &PackStorageFs) -> Result<()> {
    self.ensure_pack_keys(fs)?;

    let packs_pairs = self.get_pack_meta_pairs()?;

    async fn read_pack_contents(
      bid: usize,
      pid: usize,
      path: &PathBuf,
      keys: &PackKeys,
      fs: &PackStorageFs,
    ) -> Result<(usize, usize, PackContents)> {
      match fs.read_pack_contents(&path, &keys)? {
        Some(v) => Ok((bid, pid, v)),
        None => Ok((bid, pid, vec![])),
      }
    }

    let mut tasks = packs_pairs
      .into_iter()
      .filter(|(_, _, _, pack)| {
        matches!(pack.contents, PackContentsState::Pending)
          && matches!(pack.keys, PackKeysState::Value(_))
      })
      .map(|(bucket_id, pack_pos, _, pack)| {
        let pack_path = pack.path.to_owned();
        let pack_keys = pack.keys.expect_value().to_owned();
        let fs = fs.to_owned();
        tokio::spawn(async move {
          read_pack_contents(bucket_id, pack_pos, &pack_path, &pack_keys, &fs).await
        })
        .map_err(|e| error!("{}", e))
      });

    let task_results = block_on(join_all(tasks))
      .into_iter()
      .collect::<Result<Vec<Result<(usize, usize, PackContents)>>>>()?;

    let packs = self.packs.expect_value_mut();

    for task_result in task_results {
      let (bucket_id, pack_pos, item) = task_result?;
      if let Some(pack) = packs
        .get_mut(bucket_id)
        .and_then(|packs| packs.get_mut(pack_pos))
      {
        pack.contents = PackContentsState::Value(item);
      }
    }

    Ok(())
  }

  fn ensure_meta(&mut self, fs: &PackStorageFs) -> Result<()> {
    if matches!(self.meta, ScopeMetaState::Pending) {
      let meta = fs.read_scope_meta(ScopeMeta::get_path(&self.path))?;
      if let Some(meta) = meta {
        self.meta = ScopeMetaState::Value(meta);
      } else {
        self.meta = ScopeMetaState::Value(ScopeMeta::new(&self.path, &self.options));
      }
    }
    Ok(())
  }

  fn ensure_packs(&mut self, fs: &PackStorageFs) -> Result<()> {
    self.ensure_meta(fs)?;

    let meta = self.meta.expect_value();

    if matches!(self.packs, ScopePacksState::Value(_)) {
      return Ok(());
    }

    self.packs = ScopePacksState::Value(
      meta
        .packs
        .iter()
        .enumerate()
        .map(|(bucket_id, pack_meta_list)| {
          let bucket_dir = self.path.join(bucket_id.to_string());
          pack_meta_list
            .iter()
            .map(|pack_meta| Pack::new(bucket_dir.join(&pack_meta.name)))
            .collect_vec()
        })
        .collect_vec(),
    );

    Ok(())
  }

  fn get_pack_meta_pairs(&self) -> Result<Vec<(usize, usize, Arc<PackFileMeta>, &Pack)>> {
    let meta = self.meta.expect_value();
    let packs = self.packs.expect_value();

    Ok(
      meta
        .packs
        .iter()
        .enumerate()
        .map(|(bucket_id, pack_meta_list)| {
          let bucket_packs = packs.get(bucket_id).expect("should have bucket packs");
          pack_meta_list
            .iter()
            .enumerate()
            .map(|(pack_pos, pack_meta)| {
              (
                bucket_id,
                pack_pos,
                pack_meta.clone(),
                bucket_packs.get(pack_pos).expect("should have bucket pack"),
              )
            })
            .collect_vec()
        })
        .flatten()
        .collect_vec(),
    )
  }

  pub fn save(
    &mut self,
    updates: &mut HashMap<Vec<u8>, Option<Vec<u8>>>,
    fs: &PackStorageFs,
  ) -> Result<SavedScopeResult> {
    if !self.loaded() {
      return Err(error!("scope not loaded, run `get_all` first"));
    }

    let mut scope_meta = self.meta.take_value().expect("shoud have scope meta");
    let mut scope_packs = self.packs.take_value().expect("shoud have scope packs");

    let mut removed_files = vec![];
    let mut writed_files = vec![];

    // get changed buckets
    let bucket_updates = updates
      .into_par_iter()
      .map(|(key, value)| {
        let bucket_id = get_key_bucket_id(&key, self.options.buckets);
        (bucket_id, key, value)
      })
      .collect::<Vec<_>>()
      .into_iter()
      .fold(
        HashMap::<usize, HashMap<Arc<Vec<u8>>, Option<Arc<Vec<u8>>>>>::default(),
        |mut res, (bucket_id, key, value)| {
          res
            .entry(bucket_id)
            .or_default()
            .insert(Arc::new(key.to_owned()), value.to_owned().map(Arc::new));
          res
        },
      );

    // get dirty buckets
    let mut bucket_tasks = vec![];
    for (dirty_bucket_id, dirty_items) in bucket_updates.into_iter() {
      let dirty_bucket_packs = {
        let mut packs = HashMap::default();

        let old_dirty_bucket_metas = std::mem::take(
          scope_meta
            .packs
            .get_mut(dirty_bucket_id)
            .expect("should have bucket pack metas"),
        )
        .into_iter()
        .enumerate()
        .collect::<HashMap<_, _>>();

        let mut old_dirty_bucket_packs = std::mem::take(
          scope_packs
            .get_mut(dirty_bucket_id)
            .expect("should have bucket packs"),
        )
        .into_iter()
        .enumerate()
        .collect::<HashMap<_, _>>();

        for (key, pack_meta) in old_dirty_bucket_metas.into_iter() {
          let pack = old_dirty_bucket_packs
            .remove(&key)
            .expect("should have bucket pack");
          packs.insert(pack_meta, pack);
        }
        packs
      };

      // create item to pack mapping
      let dirty_key_to_meta_map =
        dirty_bucket_packs
          .iter()
          .fold(HashMap::default(), |mut acc, (pack_meta, pack)| {
            let PackKeysState::Value(keys) = &pack.keys else {
              return acc;
            };
            for key in keys {
              acc.insert(key.clone(), pack_meta.clone());
            }
            acc
          });

      bucket_tasks.push((
        dirty_bucket_id,
        dirty_bucket_packs,
        dirty_items,
        dirty_key_to_meta_map,
      ));
    }

    // generate dirty buckets
    let dirty_bucket_results = bucket_tasks
      .into_par_iter()
      .map(
        |(bucket_id, mut bucket_packs, mut bucket_updates, mut bucket_key_to_meta_map)| {
          let bucket_res = create_bucket_packs(
            self.path.join(bucket_id.to_string()),
            &mut bucket_packs,
            &mut bucket_updates,
            &mut bucket_key_to_meta_map,
            &self.options,
          );
          (bucket_id, bucket_res)
        },
      )
      .collect::<HashMap<_, _>>();

    let mut new_packs = vec![];

    // link remain packs to scope
    for (bucket_id, bucket_result) in dirty_bucket_results {
      for (pack_meta, pack) in bucket_result.remain_packs {
        scope_packs[bucket_id].push(pack);
        scope_meta.packs[bucket_id].push(pack_meta);
      }

      for (pack_meta, pack) in bucket_result.new_packs {
        new_packs.push((bucket_id, pack_meta, pack));
      }

      removed_files.extend(bucket_result.removed_files);
    }

    // write and link new packs
    for result in write_bucket_packs(new_packs, fs)? {
      writed_files.push(result.pack.path.clone());
      scope_packs[result.bucket_id].push(result.pack);
      scope_meta.packs[result.bucket_id].push(result.meta);
    }

    // parallelly write new meta
    fs.write_scope_meta(&scope_meta)?;
    writed_files.push(scope_meta.path.clone());

    self.packs = ScopePacksState::Value(scope_packs);
    self.meta = ScopeMetaState::Value(scope_meta);

    Ok(SavedScopeResult {
      writed_files,
      removed_files,
    })
  }
}

pub struct SavedScopeResult {
  pub writed_files: Vec<PathBuf>,
  pub removed_files: Vec<PathBuf>,
}

fn get_key_bucket_id(key: &Vec<u8>, total: usize) -> usize {
  let mut hasher = FxHasher::default();
  hasher.write(key);
  let bucket_id = usize::try_from(hasher.finish() % total as u64).expect("should get bucket id");
  bucket_id
}

fn create_new_packs(
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

struct CreateBucketPacksResult {
  new_packs: Vec<(PackFileMeta, Pack)>,
  remain_packs: Vec<(Arc<PackFileMeta>, Pack)>,
  removed_files: Vec<PathBuf>,
}

fn create_bucket_packs(
  bucket_path: PathBuf,
  bucket_packs: &mut HashMap<Arc<PackFileMeta>, Pack>,
  bucket_updates: &mut HashMap<Arc<Vec<u8>>, Option<Arc<Vec<u8>>>>,
  bucket_key_to_meta_map: &mut HashMap<Arc<Vec<u8>>, Arc<PackFileMeta>>,
  options: &PackStorageOptions,
) -> CreateBucketPacksResult {
  let mut removed_packs = HashSet::default();
  let mut insert_keys = HashSet::default();
  let mut removed_keys = HashSet::default();

  let mut removed_files = vec![];

  // TODO: try to update pack
  // let mut updated_packs = HashSet::default();
  // let mut updated_keys = HashSet::default();

  for (dirty_key, dirty_value) in bucket_updates.iter() {
    if dirty_value.is_some() {
      if let Some(pack_meta) = bucket_key_to_meta_map.get(dirty_key) {
        // update
        // updated_packs.insert(pack_meta);
        // updated_keys.insert(dirty_key)
        insert_keys.insert(dirty_key.clone());
        removed_packs.insert(pack_meta.clone());
      } else {
        // insert
        insert_keys.insert(dirty_key.clone());
      }
    } else {
      if let Some(pack_meta) = bucket_key_to_meta_map.get(dirty_key) {
        // remove
        removed_keys.insert(dirty_key.clone());
        removed_packs.insert(pack_meta.clone());
      } else {
        // not exists, do nothing
      }
    }
  }

  // pour out items from removed packs
  let mut wait_items = removed_packs
    .iter()
    .fold(vec![], |mut acc, pack_meta| {
      let old_pack = bucket_packs
        .remove(pack_meta)
        .expect("should have bucket pack");

      removed_files.push(old_pack.path.clone());

      let (PackKeysState::Value(keys), PackContentsState::Value(contents)) =
        (old_pack.keys, old_pack.contents)
      else {
        return acc;
      };
      if keys.len() != contents.len() {
        return acc;
      }
      for (content_pos, content) in keys.iter().enumerate() {
        acc.push((
          content.to_owned(),
          contents
            .get(content_pos)
            .expect("should have content")
            .to_owned(),
        ));
      }
      acc
    })
    .into_iter()
    .filter(|(key, _)| !removed_keys.contains(key))
    .filter(|(key, _)| !insert_keys.contains(key))
    .collect::<Vec<_>>();

  // add insert items
  wait_items.extend(
    insert_keys
      .iter()
      .filter_map(|key| {
        bucket_updates
          .remove(key)
          .expect("should have insert item")
          .map(|val| (key.clone(), val))
      })
      .collect::<Vec<_>>(),
  );

  let remain_packs = bucket_packs
    .into_iter()
    .filter(|(meta, _)| !removed_packs.contains(*meta))
    .map(|(meta, pack)| (meta.clone(), pack.to_owned()))
    .collect::<Vec<_>>();

  let new_packs: Vec<(PackFileMeta, Pack)> =
    create_new_packs(&mut wait_items, &bucket_path, &options);

  CreateBucketPacksResult {
    new_packs,
    remain_packs,
    removed_files,
  }
}

struct WriteBucketResult {
  bucket_id: usize,
  meta: Arc<PackFileMeta>,
  pack: Pack,
}

fn write_bucket_packs(
  packs: Vec<(usize, PackFileMeta, Pack)>,
  fs: &PackStorageFs,
) -> Result<Vec<WriteBucketResult>> {
  async fn save_pack(pack: &Pack, fs: &PackStorageFs) -> Result<String> {
    pack.write(fs)?;
    get_pack_hash(
      &fs.redirect_to_temp(&pack.path)?,
      pack.keys.expect_value(),
      fs,
    )
  }

  let save_tasks = packs.into_iter().map(|(bucket_id, meta, pack)| {
    let fs = fs.to_owned();
    tokio::spawn(async move {
      let hash = save_pack(&pack, &fs).await;
      (bucket_id, meta, pack, hash)
    })
    .map_err(|e| error!("{}", e))
  });

  let save_results = block_on(join_all(save_tasks))
    .into_iter()
    .collect::<Result<Vec<(usize, PackFileMeta, Pack, Result<String>)>>>()?;

  let mut res = vec![];
  for (bucket_id, mut meta, pack, hash_task) in save_results {
    let hash = hash_task?;
    meta.hash = hash;
    res.push(WriteBucketResult {
      bucket_id,
      meta: Arc::new(meta),
      pack,
    });
  }
  Ok(res)
}
