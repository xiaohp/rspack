use std::sync::Arc;

use futures::{future::join_all, TryFutureExt};
use rspack_error::{error, Result};

use crate::pack::{get_pack_hash, Pack, PackStorageFs};

async fn save_pack(pack: Pack, fs: Arc<PackStorageFs>) -> Result<(String, Pack)> {
  let keys = pack.keys.expect_value();
  let contents = pack.contents.expect_value();
  if keys.len() != contents.len() {
    return Err(error!("pack keys and contents length not match"));
  }
  fs.write_pack(&pack.path, keys, contents)?;
  let hash = get_pack_hash(
    &fs.redirect_to_temp(&pack.path)?,
    pack.keys.expect_value(),
    fs,
  )?;
  Ok((hash, pack))
}

pub async fn batch_write_packs(
  packs: Vec<Pack>,
  fs: Arc<PackStorageFs>,
) -> Result<Vec<(String, Pack)>> {
  let tasks = packs.into_iter().map(|pack| {
    let fs = fs.clone();
    tokio::spawn(async move { save_pack(pack, fs).await }).map_err(|e| error!("{}", e))
  });

  let writed = join_all(tasks)
    .await
    .into_iter()
    .collect::<Result<Vec<Result<(String, Pack)>>>>()?;

  let mut res = vec![];
  for hash in writed {
    res.push(hash?);
  }
  Ok(res)
}
