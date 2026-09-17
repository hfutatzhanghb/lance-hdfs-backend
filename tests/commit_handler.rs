// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "commit-handler")]
#[test]
fn test_rename_commit_handler_is_selected_type() {
    let handler = lance_hdfs_backend::rename_commit_handler();

    assert_eq!(format!("{handler:?}"), "RenameCommitHandler");
}
