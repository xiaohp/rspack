use std::{hash::Hasher, path::PathBuf, sync::Arc};

use pollster::block_on;
use rspack_error::Result;
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet, FxHasher};
use tokio::task::unconstrained;

use super::{batch_write_packs, fill_packs, Pack, PackFileMeta, PackStorageFs, PackStorageOptions};
use crate::pack::{PackContentsState, PackKeysState};

pub fn choose_bucket(key: &Vec<u8>, total: usize) -> usize {
  let mut hasher = FxHasher::default();
  hasher.write(key);
  let bucket_id = usize::try_from(hasher.finish() % total as u64).expect("should get bucket id");
  bucket_id
}

pub struct CreateBucketPacksResult {
  pub new_packs: Vec<(PackFileMeta, Pack)>,
  pub remain_packs: Vec<(Arc<PackFileMeta>, Pack)>,
  pub removed_files: Vec<PathBuf>,
}

pub fn incremental_bucket_packs(
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

  let new_packs: Vec<(PackFileMeta, Pack)> = fill_packs(&mut wait_items, &bucket_path, &options);

  CreateBucketPacksResult {
    new_packs,
    remain_packs,
    removed_files,
  }
}

pub struct WriteBucketResult {
  pub bucket_id: usize,
  pub meta: Arc<PackFileMeta>,
  pub pack: Pack,
}

pub fn write_bucket_packs(
  metas: Vec<(usize, PackFileMeta)>,
  packs: Vec<Pack>,
  fs: &PackStorageFs,
) -> Result<Vec<WriteBucketResult>> {
  let write_results = block_on(unconstrained(batch_write_packs(packs, &fs)))?;
  let mut res = vec![];
  for ((bucket_id, mut meta), (hash, pack)) in metas.into_iter().zip(write_results.into_iter()) {
    meta.hash = hash;
    res.push(WriteBucketResult {
      bucket_id,
      meta: Arc::new(meta),
      pack,
    });
  }
  Ok(res)
}
