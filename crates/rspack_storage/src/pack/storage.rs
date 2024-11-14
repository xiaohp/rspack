use std::{
  path::PathBuf,
  sync::{Arc, Mutex},
};

use itertools::Itertools;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rspack_error::{error, Result};
use rustc_hash::FxHashMap as HashMap;

use super::{PackScope, PackStorageFs, PackStorageOptions, SavedScopeResult, ScopeValidateResult};
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
    self.fs.clean_temporary()?;
    self.fs.ensure_root()?;

    // write scopes
    let mut tasks = vec![];
    for (scope_name, scope_updates) in updates {
      let scope = scopes.remove(scope_name).expect("should have scope");
      tasks.push((scope, scope_updates));
    }

    let save_results = tasks
      .into_par_iter()
      .map(move |(mut scope, mut scope_updates)| scope.save(&mut scope_updates, &self.fs))
      .collect::<Result<Vec<SavedScopeResult>>>()?;

    // move temp to cache root
    self.fs.move_temporary(
      &save_results
        .iter()
        .map(|s| s.writed_files.to_owned())
        .flatten()
        .collect_vec(),
      &save_results
        .iter()
        .map(|s| s.removed_files.to_owned())
        .flatten()
        .collect_vec(),
    )?;
    // self.fs.clean_temporary()?;

    Ok(())
  }
}
