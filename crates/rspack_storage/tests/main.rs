#[cfg(test)]
mod test_storage {

  use std::{
    collections::HashMap, fs::remove_dir_all, hash::Hasher, path::PathBuf, sync::Arc,
    time::Duration,
  };

  use futures::{executor::block_on, future::join_all, TryFutureExt};
  use rspack_error::{error, Result};
  use rspack_storage::{PackStorage, PackStorageOptions, Storage};
  use rustc_hash::FxHasher;
  use tokio::{runtime::Handle, task::JoinSet, time::sleep};

  #[test]
  fn test_basic() {
    // should not panic
    fn create_key(str: String) -> String {
      let mut hasher = FxHasher::default();
      hasher.write(str.as_bytes());
      hasher.write_usize(str.len());
      format!("{:016x}", hasher.finish())
    }
    fn create_content(str: String, repeat: usize) -> String {
      let mut hasher = FxHasher::default();
      hasher.write(str.as_bytes());
      hasher.write_usize(str.len());
      format!("{:016x}", hasher.finish()).repeat(repeat)
    }

    let storage_options = PackStorageOptions {
      buckets: 10,
      max_pack_size: 500 * 1000,
      expires: 99999999999,
    };

    async fn write_storage(options: &PackStorageOptions) {
      println!("write storage");

      let test_root = PathBuf::from("/Users/bytedance/sources/bundler-playground/test/storage");
      if test_root.exists() {
        remove_dir_all(&test_root).expect("should remove test root");
      }

      let temp_rot = PathBuf::from("/Users/bytedance/sources/bundler-playground/test/temp");
      if temp_rot.exists() {
        remove_dir_all(&temp_rot).expect("should remove test root");
      }

      let storage = Arc::new(PackStorage::new(
        PathBuf::from("/Users/bytedance/sources/bundler-playground/test/storage"),
        PathBuf::from("/Users/bytedance/sources/bundler-playground/test/temp"),
        options.clone(),
      ));

      // NOTICE: 5000 will block the test
      for idx in 0..5000 {
        storage.set(
          "scope_name",
          create_key(format!("item_key:{}", idx.to_string()))
            .as_bytes()
            .to_vec(),
          create_content(format!("item_value:{}", idx.to_string()), 500)
            .as_bytes()
            .to_vec(),
        );
      }
      match storage.idle() {
        Ok(_) => {}
        Err(e) => {
          println!("failed: {}", e.to_string());
          panic!("{:?}", e);
        }
      }
    }

    async fn modify_storage(options: &PackStorageOptions) {
      println!("modify storage");
      let storage = Arc::new(PackStorage::new(
        PathBuf::from("/Users/bytedance/sources/bundler-playground/test/storage"),
        PathBuf::from("/Users/bytedance/sources/bundler-playground/test/temp"),
        options.clone(),
      ));

      println!("asdfasdf");
      storage.get_all("scope_name");
      println!("asdfasdfaaa");

      storage.remove("scope_name", b"item_key:1");
      storage.set(
        "scope_name",
        create_key(format!("item_key:2")).as_bytes().to_vec(),
        format!("changed_value:2").as_bytes().to_vec(),
      );

      match storage.idle() {
        Ok(_) => {}
        Err(e) => {
          println!("failed: {}", e);
          panic!("{:?}", e);
        }
      }
    }

    async fn read_storage(options: &PackStorageOptions) {
      println!("read storage");
      let storage = Arc::new(PackStorage::new(
        PathBuf::from("/Users/bytedance/sources/bundler-playground/test/storage"),
        PathBuf::from("/Users/bytedance/sources/bundler-playground/test/temp"),
        options.clone(),
      ));

      let result = match storage.get_all("scope_name") {
        Ok(data) => data
          .iter()
          .map(|(k, v)| {
            (
              String::from_utf8(k.to_vec()).expect("failed"),
              String::from_utf8(v.to_vec()).expect("failed"),
            )
          })
          .collect::<HashMap<String, String>>(),
        Err(e) => {
          println!("failed: {}", e);
          panic!("{:?}", e);
        }
      };
      println!(
        "result value: {:?}",
        result.get(&create_key("item_key:2".to_string()))
      );
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(write_storage(&storage_options));
    rt.block_on(modify_storage(&storage_options));
    rt.block_on(read_storage(&storage_options));

    // NOTICE: 300 will block the test
    // async fn test_parallel() {
    //   let mut tasks = vec![];
    //   for bucket_id in 0..300 {
    //     tasks.push(
    //       tokio::spawn(async move {
    //         sleep(Duration::from_millis(100)).await;
    //         (bucket_id, bucket_id * 2)
    //       })
    //       .map_err(|e| error!("{}", e)),
    //     );
    //   }
    //   let task_results = block_on(tokio::task::unconstrained(join_all(tasks)))
    //     .into_iter()
    //     .collect::<Result<Vec<(i32, i32)>>>();
    //   println!("task restuls: {:?}", task_results);
    // }

    // rt.block_on(test_parallel());
  }

  // #[test]
  // fn case() {
  //   let ends_with_js = regex!("\\.js$");
  //   let ends_with_js_ignore_case = regex!("\\.js$", i);
  //   // case sensitive
  //   assert!(ends_with_js.test(".js"));
  //   assert!(!ends_with_js.test(".JS"));
  //   // ignore case
  //   assert!(ends_with_js_ignore_case.test(".js"));
  //   assert!(ends_with_js_ignore_case.test(".JS"));
  // }
}
