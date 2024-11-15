use std::{
  path::PathBuf,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
  },
};

use futures::future::join_all;
use itertools::Itertools;
use rspack_error::{error, Result};
use rustc_hash::FxHashMap as HashMap;

use super::TaskQueue;
use crate::pack::{PackScope, SavedScopeResult, ScopeValidateResult};
use crate::{
  pack::{batch_move_files, batch_remove_files, PackStorageFs, ScopeUpdates},
  PackStorageOptions,
};

type ScopeMap = HashMap<&'static str, PackScope>;

#[derive(Debug)]
pub struct ScopeManager {
  root: Arc<PathBuf>,
  options: Arc<PackStorageOptions>,
  fs: Arc<PackStorageFs>,
  alive: Arc<AtomicBool>,
  update_queue: Arc<Mutex<Vec<ScopeUpdates>>>,
  scopes: Arc<Mutex<ScopeMap>>,
  queue: TaskQueue,
}

impl ScopeManager {
  pub fn new(root: PathBuf, options: PackStorageOptions, fs: PackStorageFs) -> Self {
    ScopeManager {
      root: Arc::new(root),
      options: Arc::new(options),
      fs: Arc::new(fs),
      alive: Arc::new(AtomicBool::new(false)),
      update_queue: Default::default(),
      scopes: Default::default(),
      queue: TaskQueue::new(),
    }
  }
  pub fn update(&mut self, updates: ScopeUpdates) {
    self.update_queue.lock().unwrap().push(updates);
    if self.alive.load(Ordering::Relaxed) {
      return;
    }
    self.start();
  }

  fn start(&mut self) {
    self.alive.store(true, Ordering::Relaxed);

    let scopes_mutex = self.scopes.clone();
    let update_queue_mutex = self.update_queue.clone();
    let root = self.root.clone();
    let options = self.options.clone();
    let fs = self.fs.clone();

    self.queue.add_task(|| {
      Box::pin(async move {
        let scopes = std::mem::take(&mut *scopes_mutex.lock().unwrap());
        let update_queue = std::mem::take(&mut *update_queue_mutex.lock().unwrap());
        match save_scopes(root, options, fs, scopes, update_queue).await {
          Ok(new_scopes) => {
            *scopes_mutex.lock().unwrap() = new_scopes;
          }
          Err(e) => println!("{}", e),
        };
      })
    });
  }

  pub fn get_all(&mut self, name: &'static str) -> Result<Vec<(Arc<Vec<u8>>, Arc<Vec<u8>>)>> {
    let mut scopes = self.scopes.lock().unwrap();
    let scope = scopes
      .entry(name)
      .or_insert_with(|| PackScope::new(name, &self.root, self.options.clone()));

    match scope.validate(&self.options, self.fs.clone()) {
      Ok(validate) => match validate {
        ScopeValidateResult::Valid => scope.get_contents(self.fs.clone()),
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
}

fn merge_updates(updates: Vec<ScopeUpdates>) -> ScopeUpdates {
  updates
    .into_iter()
    .fold(HashMap::default(), |mut acc, update| {
      for (scope_name, scope_updates) in update {
        acc.entry(scope_name).or_default().extend(scope_updates);
      }
      acc
    })
}

async fn save_scopes(
  root: Arc<PathBuf>,
  options: Arc<PackStorageOptions>,
  fs: Arc<PackStorageFs>,
  scopes: ScopeMap,
  updates: Vec<ScopeUpdates>,
) -> Result<ScopeMap> {
  let mut updates = merge_updates(updates);
  let mut scopes = scopes;

  for (scope_name, _) in updates.iter() {
    scopes
      .entry(scope_name)
      .or_insert_with(|| PackScope::empty(scope_name, &root, options.clone()));
  }

  // prepare
  before_save(fs.clone()).await?;

  // write scopes
  let mut scopes = scopes.into_iter().collect_vec();
  let tasks = join_all(
    scopes
      .iter_mut()
      .map(|(name, scope)| {
        let scope_updates = updates.remove(name).unwrap_or_default();
        scope.save(scope_updates, fs.clone())
      })
      .collect_vec(),
  );
  let (writed_files, removed_files) = tasks
    .await
    .into_iter()
    .collect::<Result<Vec<SavedScopeResult>>>()?
    .into_iter()
    .fold((vec![], vec![]), |mut acc, s| {
      acc.0.extend(s.writed_files);
      acc.1.extend(s.removed_files);
      acc
    });

  // move temp to cache root
  after_save(writed_files, removed_files, fs.clone()).await?;

  Ok(scopes.into_iter().collect())
}

async fn before_save(fs: Arc<PackStorageFs>) -> Result<()> {
  fs.clean_temporary()?;
  fs.ensure_root()?;
  Ok(())
}

async fn after_save(
  writed_files: Vec<PathBuf>,
  removed_files: Vec<PathBuf>,
  fs: Arc<PackStorageFs>,
) -> Result<()> {
  batch_move_files(writed_files, fs.clone()).await?;
  batch_remove_files(removed_files, fs.clone()).await?;

  fs.clean_temporary()?;
  // self.fs.clean_temporary()?;
  Ok(())
}
