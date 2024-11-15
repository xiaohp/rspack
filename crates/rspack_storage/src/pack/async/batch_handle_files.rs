use std::{path::PathBuf, sync::Arc};

use futures::{future::join_all, TryFutureExt};
use rspack_error::{error, Result};

use crate::pack::PackStorageFs;

pub async fn batch_remove_files(files: Vec<PathBuf>, fs: Arc<PackStorageFs>) -> Result<()> {
  let tasks = files.into_iter().map(|path| {
    let fs = fs.to_owned();
    tokio::spawn(async move { fs.remove_file(&path).await }).map_err(|e| error!("{}", e))
  });

  join_all(tasks)
    .await
    .into_iter()
    .collect::<Result<Vec<Result<()>>>>()?;

  Ok(())
}

pub async fn batch_move_files(files: Vec<PathBuf>, fs: Arc<PackStorageFs>) -> Result<()> {
  let tasks = files.into_iter().map(|path| {
    let fs = fs.to_owned();
    tokio::spawn(async move { fs.move_file(&path).await }).map_err(|e| error!("{}", e))
  });

  join_all(tasks)
    .await
    .into_iter()
    .collect::<Result<Vec<Result<()>>>>()?;

  Ok(())
}
