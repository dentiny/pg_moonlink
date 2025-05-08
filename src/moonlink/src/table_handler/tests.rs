use super::test_utils::*;

#[tokio::test]
async fn test_table_handler() {
    let mut env = TestEnvironment::new();

    env.append_row(1, "John", 30, None).await;
    env.commit(1).await;

    env.set_readable_lsn_with_cap(1, 100); // table_commit_lsn = 1, replication_lsn = 100
    env.verify_snapshot(1, &[1]).await;
    env.verify_snapshot(100, &[1]).await; // Reading at a higher LSN should still see data from LSN 1

    env.shutdown().await;
    println!("All table handler tests passed!");
}

#[tokio::test]
async fn test_table_handler_flush() {
    let mut env = TestEnvironment::new();

    let rows_data = vec![(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)];
    for (id, name, age) in rows_data {
        env.append_row(id, name, age, None).await;
    }

    env.commit(1).await;
    env.flush_table(1).await;

    env.set_readable_lsn_with_cap(1, 100);
    env.verify_snapshot(1, &[1, 2, 3]).await;

    env.shutdown().await;
    println!("All table handler flush tests passed!");
}

#[tokio::test]
async fn test_streaming_append_and_commit() {
    let mut env = TestEnvironment::new();
    let xact_id = 101;

    env.append_row(10, "Transaction-User", 25, Some(xact_id))
        .await;
    env.stream_commit(101, xact_id).await;

    env.set_readable_lsn(101);
    env.verify_snapshot(101, &[10]).await;

    env.shutdown().await;
}

#[tokio::test]
async fn test_streaming_delete() {
    let mut env = TestEnvironment::new();
    let xact_id = 101;

    env.append_row(10, "Transaction-User1", 25, Some(xact_id))
        .await;
    env.append_row(11, "Transaction-User2", 30, Some(xact_id))
        .await;

    // LSN for delete op (100) can be different from the stream commit LSN (101)
    env.delete_row(10, "Transaction-User1", 25, 100, Some(xact_id))
        .await;
    env.stream_commit(101, xact_id).await;

    env.set_readable_lsn(101);
    env.verify_snapshot(101, &[11]).await;

    env.shutdown().await;
}

#[tokio::test]
async fn test_streaming_abort() {
    let mut env = TestEnvironment::new();

    // Baseline data
    let baseline_xact_id = 100;
    env.append_row(1, "Baseline-User", 20, Some(baseline_xact_id))
        .await;
    env.stream_commit(100, baseline_xact_id).await;

    // Set table_commit_tx to allow ReadStateManager to know LSN 100 is committed.
    env.set_table_commit_lsn(100);

    // Transaction to be aborted
    let abort_xact_id = 102;
    env.append_row(20, "UserToAbort", 40, Some(abort_xact_id))
        .await;
    env.stream_abort(abort_xact_id).await;

    // Now enable reading up to LSN 100 by setting replication_tx.
    // The target_lsn for read is 100.
    env.set_replication_lsn(100);
    env.verify_snapshot(100, &[1]).await; // Should only see baseline data.

    env.shutdown().await;
}

#[tokio::test]
async fn test_concurrent_streaming_transactions() {
    let mut env = TestEnvironment::new();
    let xact_id_1 = 103; // Will be committed
    let xact_id_2 = 104; // Will be aborted

    env.append_row(30, "Transaction1-User", 35, Some(xact_id_1))
        .await;
    env.append_row(40, "Transaction2-User", 45, Some(xact_id_2))
        .await;

    env.stream_commit(103, xact_id_1).await; // Commit transaction 1 at LSN 103
    env.stream_abort(xact_id_2).await; // Abort transaction 2

    env.set_readable_lsn(103); // Make LSN 103 readable
    env.verify_snapshot(103, &[30]).await; // Verify only data from committed transaction 1

    env.shutdown().await;
}
