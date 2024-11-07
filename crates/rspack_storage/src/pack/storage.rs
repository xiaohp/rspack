use std::sync::Mutex;

use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rspack_error::{error, Result};
use rustc_hash::FxHashMap as HashMap;

use super::{save_scope, PackScope, PackStorageOptions};
use crate::Storage;

#[derive(Debug)]
pub struct PackStorage {
  options: PackStorageOptions,
  scopes: Mutex<HashMap<&'static str, PackScope>>,
  scope_updates: Mutex<HashMap<&'static str, HashMap<Vec<u8>, Option<Vec<u8>>>>>,
}

impl PackStorage {
  pub fn new(options: PackStorageOptions) -> Self {
    Self {
      options,
      scopes: Default::default(),
      scope_updates: Default::default(),
    }
  }
}

impl Storage for PackStorage {
  fn get_all(&self, name: &'static str) -> Result<Vec<(&Vec<u8>, &Vec<u8>)>> {
    let mut scopes = self.scopes.lock().unwrap();
    let scope = scopes
      .entry(name)
      .or_insert_with(|| PackScope::new(self.options.location.join(name)));

    let is_valid = scope.validate(&self.options)?;
    if is_valid {
      // scope.get_contents()
      Err(error!("scope"))
    } else {
      Err(error!("scope is inconsistent"))
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
    let options = self.options.clone();
    let mut updates =
      std::mem::replace(&mut *self.scope_updates.lock().unwrap(), Default::default());
    let mut scopes = std::mem::replace(&mut *self.scopes.lock().unwrap(), Default::default());
    for (scope_name, _) in &updates {
      scopes
        .entry(scope_name)
        .or_insert_with(|| PackScope::new(self.options.location.join(scope_name)));
    }

    let new_scopes = save(&mut scopes, &mut updates, options)?;
    std::mem::replace(&mut *self.scopes.lock().unwrap(), new_scopes);
    Ok(())
  }
}

fn save(
  scopes: &mut HashMap<&'static str, PackScope>,
  updates: &mut HashMap<&'static str, HashMap<Vec<u8>, Option<Vec<u8>>>>,
  options: PackStorageOptions,
) -> Result<HashMap<&'static str, PackScope>> {
  let mut tasks = vec![];

  for (scope_name, map) in updates {
    let scope = scopes.remove(scope_name).unwrap_or_else(|| unreachable!());
    tasks.push((scope_name, map, scope));
  }

  let new_scopes = tasks
    .into_par_iter()
    .map(move |(scope_name, mut map, mut scope)| {
      save_scope(scope_name, &mut scope, &mut map, &options)
    })
    .collect::<Result<HashMap<&'static str, PackScope>>>()?;

  Ok(new_scopes)
}
