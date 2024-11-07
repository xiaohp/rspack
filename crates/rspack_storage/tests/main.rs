#[cfg(test)]
mod test_storage {

  use std::{path::PathBuf, sync::Arc};

  use rspack_storage::FsStorage;

  #[test]
  fn test_basic() {
    // should not panic

    let storage = Arc::new(FsStorage::new(PathBuf::from("")));
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
