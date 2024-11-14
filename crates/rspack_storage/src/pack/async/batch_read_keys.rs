use std::path::PathBuf;

use futures::{future::join_all, TryFutureExt};
use rspack_error::{error, Result};

use crate::pack::{PackKeys, PackStorageFs};

pub async fn batch_read_keys(
  candidates: Vec<PathBuf>,
  fs: &PackStorageFs,
) -> Result<Vec<PackKeys>> {
  let tasks = candidates.into_iter().map(|path| {
    let fs = fs.to_owned();
    tokio::spawn(async move { fs.read_pack_keys(&path) }).map_err(|e| error!("{}", e))
  });

  let readed = join_all(tasks)
    .await
    .into_iter()
    .collect::<Result<Vec<Result<Option<PackKeys>>>>>()?;

  let mut res = vec![];
  for keys in readed {
    res.push(keys?.unwrap_or_default());
  }
  Ok(res)
}
