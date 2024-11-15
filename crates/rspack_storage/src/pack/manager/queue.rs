use std::{
  collections::VecDeque,
  fmt::Debug,
  sync::{Arc, Mutex},
};

use futures::future::BoxFuture;
use tokio::sync::mpsc::{self, UnboundedSender};

pub struct TaskQueue {
  queue: Arc<Mutex<VecDeque<BoxFuture<'static, ()>>>>,
  sender: UnboundedSender<()>,
}

impl Debug for TaskQueue {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "TaskQueue {{... }}")
  }
}

impl TaskQueue {
  pub fn new() -> Self {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let queue = Arc::new(Mutex::new(VecDeque::<BoxFuture<'static, ()>>::new()));
    let queue_clone = queue.clone();

    tokio::spawn(async move {
      while receiver.recv().await.is_some() {
        if let Some(task) = queue_clone.lock().unwrap().pop_front() {
          tokio::spawn(task);
        }
      }
    });

    TaskQueue { queue, sender }
  }

  pub fn add_task<F>(&self, task: F)
  where
    F: FnOnce() -> BoxFuture<'static, ()> + Send + 'static,
  {
    self.queue.lock().unwrap().push_back(task());
    self.sender.send(()).unwrap();
  }
}
