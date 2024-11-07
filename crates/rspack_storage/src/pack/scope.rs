use std::{
  hash::Hasher,
  path::PathBuf,
  time::{SystemTime, UNIX_EPOCH},
};

use futures::{executor::block_on, future::join_all, TryFutureExt};
use itertools::Itertools;
use rspack_error::{error, miette::Error, Result};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet, FxHasher};

use super::{
  get_keys_hash, validate_pack, Pack, PackContents, PackContentsState, PackFileMeta,
  PackStorageOptions, ScopeMeta,
};
use crate::pack::{PackKeys, PackKeysState};

#[derive(Debug, Default)]
pub enum ScopeMetaState {
  #[default]
  Pending,
  Failed(Error),
  Value(ScopeMeta),
}

impl ScopeMetaState {
  pub fn expect_value(&self) -> Result<&ScopeMeta> {
    match self {
      ScopeMetaState::Value(v) => Ok(v),
      ScopeMetaState::Failed(e) => Err(error!("{}", e)),
      ScopeMetaState::Pending => Err(error!("scope meta is not ready")),
    }
  }
  pub fn expect_value_mut(&mut self) -> Result<&mut ScopeMeta> {
    match self {
      ScopeMetaState::Value(v) => Ok(v),
      ScopeMetaState::Failed(e) => Err(error!("{}", e)),
      ScopeMetaState::Pending => Err(error!("scope meta is not ready")),
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

#[derive(Debug, Default)]
pub enum ScopePacksState {
  #[default]
  Pending,
  Failed(Error),
  Value(ScopePacks),
}

impl ScopePacksState {
  pub fn expect_value(&self) -> Result<&ScopePacks> {
    match self {
      ScopePacksState::Value(v) => Ok(v),
      ScopePacksState::Failed(e) => Err(error!("{}", e)),
      ScopePacksState::Pending => Err(error!("scope meta is not ready")),
    }
  }
  pub fn expect_value_mut(&mut self) -> Result<&mut ScopePacks> {
    match self {
      ScopePacksState::Value(v) => Ok(v),
      ScopePacksState::Failed(e) => Err(error!("{}", e)),
      ScopePacksState::Pending => Err(error!("scope meta is not ready")),
    }
  }
  pub fn take_value(&mut self) -> Option<ScopePacks> {
    match self {
      ScopePacksState::Value(v) => Some(std::mem::take(&mut *v)),
      _ => None,
    }
  }
}

#[derive(Debug)]
pub struct PackScope {
  pub path: PathBuf,
  pub meta: ScopeMetaState,
  pub packs: ScopePacksState,
}

impl PackScope {
  pub fn new(path: PathBuf) -> Self {
    Self {
      path,
      meta: ScopeMetaState::Pending,
      packs: ScopePacksState::Pending,
    }
  }

  pub fn get_contents(&mut self) -> Result<Vec<(&Vec<u8>, &Vec<u8>)>> {
    self.ensure_meta()?;
    self.ensure_pack_keys()?;
    self.ensure_pack_contents()?;

    if let ScopePacksState::Value(packs) = &self.packs {
      Ok(
        packs
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
                    .map(|(index, key)| (key, &contents[index]))
                    .collect_vec(),
                );
              }
            }
            None
          })
          .flatten()
          .collect_vec(),
      )
    } else {
      unreachable!()
    }
  }

  pub fn validate(&mut self, options: &PackStorageOptions) -> Result<bool> {
    self.ensure_meta()?;

    let ScopeMetaState::Value(meta) = &self.meta else {
      unreachable!()
    };

    // validate meta
    if meta.buckets != options.buckets || meta.max_pack_size != options.max_pack_size {
      return Err(error!("cache options changed"));
    }

    let current_time = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .map_err(|e| error!("get current time failed: {}", e))?
      .as_secs();

    if current_time - meta.last_modified > options.expires {
      return Err(error!("cache meta expired"));
    }

    // validate packs
    self.ensure_pack_keys()?;
    let validate = self.validate_packs()?;

    Ok(validate)
  }

  fn validate_packs(&self) -> Result<bool> {
    let packs = self.get_pack_meta_pairs()?;

    let tasks = packs
      .iter()
      .filter(|(_, _, _, pack)| matches!(pack.keys, PackKeysState::Value(_)))
      .map(|(_, _, pack_meta, pack)| {
        let PackKeysState::Value(keys) = &pack.keys else {
          unreachable!()
        };
        let pack_path = self.path.join(&pack_meta.name);
        let pack_hash = pack_meta.hash.to_owned();
        let pack_keys = keys.to_owned();
        tokio::spawn(async {
          match validate_pack(pack_path, pack_keys, pack_hash) {
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

  fn ensure_pack_keys(&mut self) -> Result<()> {
    let packs_pairs = self.get_pack_meta_pairs()?;

    let tasks = packs_pairs
      .iter()
      .filter(|(_, _, _, pack)| matches!(pack.keys, PackKeysState::Pending))
      .map(|(bucket_id, pack_pos, _, pack)| {
        let pack_path = pack.path.to_owned();
        let bid = bucket_id.to_owned();
        let pid = pack_pos.to_owned();
        tokio::spawn(async move {
          match Pack::read_keys(&pack_path) {
            Ok(v) => (bid, pid, PackKeysState::Value(v)),
            Err(e) => (bid, pid, PackKeysState::Failed(e)),
          }
        })
        .map_err(|e| error!("{}", e))
      });

    let pack_keys = block_on(join_all(tasks))
      .into_iter()
      .collect::<Result<Vec<(usize, usize, PackKeysState)>>>()?;

    let packs = self.packs.expect_value_mut()?;

    for (bucket_id, pack_pos, item) in pack_keys {
      if let Some(pack) = packs
        .get_mut(bucket_id)
        .and_then(|packs| packs.get_mut(pack_pos))
      {
        pack.keys = item;
      }
    }

    Ok(())
  }

  fn ensure_pack_contents(&mut self) -> Result<()> {
    let packs_pairs = self.get_pack_meta_pairs()?;

    let tasks = packs_pairs
      .iter()
      .filter(|(_, _, _, pack)| {
        matches!(pack.contents, PackContentsState::Pending)
          && matches!(pack.keys, PackKeysState::Value(_))
      })
      .map(|(bucket_id, pack_pos, _, pack)| {
        let pack_path = pack.path.to_owned();
        let bid = bucket_id.to_owned();
        let pid = pack_pos.to_owned();
        let pack_keys = pack
          .keys
          .expect_value()
          .unwrap_or_else(|_| unreachable!())
          .to_owned();
        tokio::spawn(async move {
          match Pack::read_contents(&pack_path, &pack_keys) {
            Ok(v) => (bid, pid, PackContentsState::Value(v)),
            Err(e) => (bid, pid, PackContentsState::Failed(e)),
          }
        })
        .map_err(|e| error!("{}", e))
      });

    let pack_contents = block_on(join_all(tasks))
      .into_iter()
      .collect::<Result<Vec<(usize, usize, PackContentsState)>>>()?;

    let packs = self.packs.expect_value_mut()?;

    for (bucket_id, pack_pos, item) in pack_contents {
      if let Some(pack) = packs
        .get_mut(bucket_id)
        .and_then(|packs| packs.get_mut(pack_pos))
      {
        pack.contents = item;
      }
    }

    Ok(())
  }

  fn ensure_meta(&mut self) -> Result<()> {
    if matches!(self.meta, ScopeMetaState::Pending) {
      self.meta = match ScopeMeta::read(&self.path) {
        Ok(v) => ScopeMetaState::Value(v),
        Err(e) => ScopeMetaState::Failed(e),
      };
    }

    match &self.meta {
      ScopeMetaState::Pending => unreachable!(),
      ScopeMetaState::Failed(e) => {
        self.packs = ScopePacksState::Failed(error!("load scope meta failed"));
        return Err(error!("{}", e));
      }
      ScopeMetaState::Value(meta) => match &self.packs {
        ScopePacksState::Pending => {
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
        ScopePacksState::Failed(e) => Err(error!("{}", e)),
        ScopePacksState::Value(_) => Ok(()),
      },
    }
  }

  fn get_pack_meta_pairs(&self) -> Result<Vec<(usize, usize, &PackFileMeta, &Pack)>> {
    let ScopeMetaState::Value(meta) = &self.meta else {
      return Err(error!("meta not ready"));
    };
    let ScopePacksState::Value(packs) = &self.packs else {
      return Err(error!("packs not ready"));
    };

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
                pack_meta,
                bucket_packs.get(pack_pos).expect("should have bucket pack"),
              )
            })
            .collect_vec()
        })
        .flatten()
        .collect_vec(),
    )
  }
}

pub fn save_scope(
  name: &'static str,
  scope: &mut PackScope,
  updates: &mut HashMap<Vec<u8>, Option<Vec<u8>>>,
  options: &PackStorageOptions,
) -> Result<(&'static str, PackScope)> {
  let scope_path = std::mem::take(&mut scope.path);

  let mut old_meta = scope
    .meta
    .take_value()
    .map(|v| v.packs)
    .unwrap_or_default()
    .into_iter()
    .map(|metas| metas.into_iter().enumerate().collect::<HashMap<_, _>>())
    .enumerate()
    .collect::<HashMap<_, _>>();
  let mut old_packs = scope
    .packs
    .take_value()
    .unwrap_or_default()
    .into_iter()
    .map(|packs| packs.into_iter().enumerate().collect::<HashMap<_, _>>())
    .enumerate()
    .collect::<HashMap<_, _>>();

  let mut new_scope = PackScope::new(scope_path.clone());
  let mut new_scope_meta = ScopeMeta::new(options);
  let mut new_scope_packs = Vec::with_capacity(options.buckets);
  for _ in 0..options.buckets {
    new_scope_packs.push(vec![]);
  }

  // get changed buckets
  let dirty_buckets: HashMap<usize, Vec<Vec<u8>>> =
    updates
      .keys()
      .cloned()
      .fold(HashMap::default(), |mut acc, key| {
        acc
          .entry(get_key_bucket_id(&key, options.buckets))
          .or_default()
          .push(key);
        acc
      });

  // save changed buckets
  for (dirty_bucket_id, dirty_keys) in dirty_buckets {
    // get packs in bucket

    let mut dirty_bucket_packs = {
      let mut packs = HashMap::default();
      let old_dirty_bucket_metas = old_meta.remove(&dirty_bucket_id).unwrap_or_default();
      let mut old_dirty_bucket_packs = old_packs.remove(&dirty_bucket_id).unwrap_or_default();

      for (key, pack_meta) in old_dirty_bucket_metas.into_iter() {
        let pack = old_dirty_bucket_packs
          .remove(&key)
          .unwrap_or_else(|| unreachable!());
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

    let mut removed_packs = HashSet::default();
    let mut insert_keys = HashSet::default();
    let mut removed_keys = HashSet::default();

    // TODO: try to update pack
    // let mut updated_packs = HashSet::default();
    // let mut updated_keys = HashSet::default();

    for data_key in dirty_keys {
      let dirty_value = updates.get(&data_key).unwrap_or_else(|| unreachable!());
      if dirty_value.is_some() {
        if let Some(pack_meta) = dirty_key_to_meta_map.get(&data_key) {
          // update
          // updated_packs.insert(pack_meta);
          // updated_keys.insert(data_key)
          insert_keys.insert(data_key);
          removed_packs.insert(pack_meta);
        } else {
          // insert
          insert_keys.insert(data_key);
        }
      } else {
        if let Some(pack_meta) = dirty_key_to_meta_map.get(&data_key) {
          // remove
          removed_keys.insert(data_key);
          removed_packs.insert(pack_meta);
        } else {
          // not exists, do nothing
        }
      }
    }

    // pour out items from removed packs
    let mut wait_items = removed_packs
      .iter()
      .fold(vec![], |mut acc, pack_meta| {
        let old_pack = dirty_bucket_packs
          .remove(pack_meta)
          .unwrap_or_else(|| unreachable!());

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
              .unwrap_or_else(|| unreachable!())
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
        .into_iter()
        .filter_map(|key| {
          updates
            .remove(&key)
            .unwrap_or_else(|| unreachable!())
            .map(|val| (key.clone(), val))
        })
        .collect::<Vec<_>>(),
    );

    let remain_packs = dirty_bucket_packs
      .into_iter()
      .filter(|(meta, _)| !removed_packs.contains(&meta))
      .collect::<Vec<_>>();
    for (pack_meta, pack) in remain_packs {
      new_scope_packs[dirty_bucket_id].push(pack);
      new_scope_meta.packs[dirty_bucket_id].push(pack_meta.to_owned());
    }

    let new_packs: Vec<(PackFileMeta, Pack)> = create_new_packs(
      &mut wait_items,
      &scope_path.join(dirty_bucket_id.to_string()),
      options,
    );
    for (pack_meta, pack) in new_packs {
      new_scope_packs[dirty_bucket_id].push(pack);
      new_scope_meta.packs[dirty_bucket_id].push(pack_meta);
    }
  }

  for pack in new_scope_packs.iter().flatten() {
    pack.write()?;
  }
  new_scope_meta.write(&scope_path)?;

  new_scope.packs = ScopePacksState::Value(new_scope_packs);
  new_scope.meta = ScopeMetaState::Value(new_scope_meta);

  Ok((name, new_scope))
}

fn get_key_bucket_id(key: &Vec<u8>, total: usize) -> usize {
  let mut hasher = FxHasher::default();
  hasher.write(key);
  let bucket_id = usize::try_from(hasher.finish() % total as u64).expect("should get bucket id");
  bucket_id
}

fn create_new_packs(
  items: &mut Vec<(Vec<u8>, Vec<u8>)>,
  dir: &PathBuf,
  options: &PackStorageOptions,
) -> Vec<(PackFileMeta, Pack)> {
  let mut new_packs = vec![];
  loop {
    let mut pack_keys: PackKeys = vec![];
    let mut pack_contents: PackContents = vec![];
    let mut pack_size = 0_usize;

    loop {
      if items.len() == 0
        || (pack_size + items.first().unwrap_or_else(|| unreachable!()).1.len()
          > options.max_pack_size)
      {
        break;
      }
      let (key, value) = items.pop().unwrap_or_else(|| unreachable!());
      pack_size += value.len();
      pack_keys.push(key);
      pack_contents.push(value);
    }

    if !pack_keys.is_empty() {
      let file_name = get_keys_hash(&pack_keys);
      let mut new_pack = Pack::new(dir.join(file_name.clone()));
      new_pack.keys = PackKeysState::Value(pack_keys);
      new_pack.contents = PackContentsState::Value(pack_contents);
      new_packs.push((
        PackFileMeta {
          name: file_name,
          hash: Default::default(),
        },
        new_pack,
      ));
    }

    if items.len() == 0 {
      break;
    }
  }

  new_packs
}
