use std::{
  path::PathBuf,
  sync::{Arc, Mutex},
};

use rspack_error::Result;
use rustc_hash::FxHashMap as HashMap;

use super::{PackStorageFs, PackStorageOptions, ScopeManager};
use crate::Storage;

pub type ScopeUpdates = HashMap<&'static str, HashMap<Vec<u8>, Option<Vec<u8>>>>;
#[derive(Debug)]
pub struct PackStorage {
  manager: Mutex<ScopeManager>,
  updates: Mutex<ScopeUpdates>,
}

impl PackStorage {
  pub fn new(root: PathBuf, temp: PathBuf, options: PackStorageOptions) -> Self {
    let fs = PackStorageFs::new(root.clone(), temp.clone());
    Self {
      manager: Mutex::new(ScopeManager::new(root, options, fs)),
      updates: Default::default(),
    }
  }
}

impl Storage for PackStorage {
  fn get_all(&self, name: &'static str) -> Result<Vec<(Arc<Vec<u8>>, Arc<Vec<u8>>)>> {
    self.manager.lock().unwrap().get_all(name)
  }
  fn set(&self, scope: &'static str, key: Vec<u8>, value: Vec<u8>) {
    let mut inner = self.updates.lock().unwrap();
    let scope_map = inner.entry(scope).or_default();
    scope_map.insert(key, Some(value));
  }
  fn remove(&self, scope: &'static str, key: &[u8]) {
    let mut inner = self.updates.lock().unwrap();
    let scope_map = inner.entry(scope).or_default();
    scope_map.insert(key.to_vec(), None);
  }
  fn idle(&self) -> Result<()> {
    let updates = std::mem::replace(&mut *self.updates.lock().unwrap(), Default::default());
    let mut manager = self.manager.lock().unwrap();
    manager.update(updates);

    Ok(())
  }
}
