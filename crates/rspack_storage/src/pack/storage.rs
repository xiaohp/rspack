use std::{
  path::PathBuf,
  sync::{Arc, Mutex},
};

use futures::executor::block_on;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rspack_error::{error, Result};
use rustc_hash::FxHashMap as HashMap;
use tokio::task::unconstrained;

use super::{
  batch_move_files, batch_remove_files, PackScope, PackStorageFs, PackStorageOptions,
  SavedScopeResult, ScopeValidateResult,
};
use crate::Storage;

#[derive(Debug)]
pub struct PackStorage {
  root: PathBuf,
  fs: PackStorageFs,
  options: PackStorageOptions,
  scopes: Mutex<HashMap<&'static str, PackScope>>,
  scope_updates: Mutex<HashMap<&'static str, HashMap<Vec<u8>, Option<Vec<u8>>>>>,
}

impl PackStorage {
  pub fn new(root: PathBuf, temp: PathBuf, options: PackStorageOptions) -> Self {
    let fs = PackStorageFs::new(root.clone(), temp.clone());
    Self {
      root,
      options,
      fs,
      scopes: Default::default(),
      scope_updates: Default::default(),
    }
  }
}

impl Storage for PackStorage {
  fn get_all(&self, name: &'static str) -> Result<Vec<(Arc<Vec<u8>>, Arc<Vec<u8>>)>> {
    let mut scopes = self.scopes.lock().unwrap();
    let scope = scopes
      .entry(name)
      .or_insert_with(|| PackScope::new(name, &self.root, self.options.clone()));

    match scope.validate(&self.options, &self.fs) {
      Ok(validate) => match validate {
        ScopeValidateResult::Valid => scope.get_contents(&self.fs),
        ScopeValidateResult::Invalid(reason) => {
          scopes.clear();
          Err(error!("cache is not validate: {}", reason))
        }
      },
      Err(e) => {
        scopes.clear();
        Err(error!("cache is not validate: {}", e))
      }
    }
  }
  fn set(&self, scope: &'static str, key: Vec<u8>, value: Vec<u8>) {
    let mut inner = self.scope_updates.lock().unwrap();
    let scope_map = inner.entry(scope).or_default();
    scope_map.insert(key, Some(value));
  }
  fn remove(&self, scope: &'static str, key: &[u8]) {
    let mut inner = self.scope_updates.lock().unwrap();
    let scope_map = inner.entry(scope).or_default();
    scope_map.insert(key.to_vec(), None);
  }
  fn idle(&self) -> Result<()> {
    let updates = std::mem::replace(&mut *self.scope_updates.lock().unwrap(), Default::default());
    let mut scopes = self.scopes.lock().expect("should get mutex scopes lock");

    for (scope_name, _) in updates.iter() {
      scopes
        .entry(scope_name)
        .or_insert_with(|| PackScope::empty(scope_name, &self.root, self.options.clone()));
    }

    // prepare
    block_on(unconstrained(async move { before_save(&self.fs).await }))
      .map_err(|e| error!("{}", e))?;

    // write scopes
    let mut tasks = vec![];
    for (scope_name, scope_updates) in updates {
      let scope = scopes.remove(scope_name).expect("should have scope");
      tasks.push((scope, scope_updates));
    }

    let (writed_files, removed_files) = tasks
      .into_par_iter()
      .map(move |(mut scope, mut scope_updates)| scope.save(&mut scope_updates, &self.fs))
      .collect::<Result<Vec<SavedScopeResult>>>()?
      .into_iter()
      .fold((vec![], vec![]), |mut acc, s| {
        acc.0.extend(s.writed_files);
        acc.1.extend(s.removed_files);
        acc
      });

    // move temp to cache root
    block_on(unconstrained(async move {
      after_save(writed_files, removed_files, &self.fs).await
    }))
    .map_err(|e| error!("{}", e))?;

    Ok(())
  }
}

async fn before_save(fs: &PackStorageFs) -> Result<()> {
  fs.clean_temporary()?;
  fs.ensure_root()?;
  Ok(())
}

async fn after_save(
  writed_files: Vec<PathBuf>,
  removed_files: Vec<PathBuf>,
  fs: &PackStorageFs,
) -> Result<()> {
  batch_move_files(writed_files, &fs).await?;
  batch_remove_files(removed_files, &fs).await?;

  fs.clean_temporary()?;
  // self.fs.clean_temporary()?;
  Ok(())
}
