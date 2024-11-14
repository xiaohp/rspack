use std::path::PathBuf;

use futures::{future::join_all, TryFutureExt};
use rspack_error::{error, Result};

use crate::pack::{get_pack_hash, PackKeys, PackStorageFs};

pub struct PackValidateCandidate {
  pub path: PathBuf,
  pub hash: String,
  pub keys: PackKeys,
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

pub async fn batch_validate(
  candidates: Vec<PackValidateCandidate>,
  fs: &PackStorageFs,
) -> Result<Vec<bool>> {
  let tasks = candidates.into_iter().map(|pack| {
    let fs = fs.to_owned();
    tokio::spawn(async move {
      match validate_pack(&pack.hash, &pack.path, &pack.keys, &fs) {
        Ok(res) => res,
        Err(_) => false,
      }
    })
    .map_err(|e| error!("{}", e))
  });

  join_all(tasks)
    .await
    .into_iter()
    .collect::<Result<Vec<bool>>>()
}
