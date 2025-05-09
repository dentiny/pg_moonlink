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

#[tokio::test]
async fn test_stream_delete_unflushed_non_streamed_row() {
    let mut env = TestEnvironment::new();

    // Define LSNs and transaction ID for clarity
    let initial_insert_lsn = 10; // LSN for the non-streaming insert
    let stream_xact_id = 101; // Transaction ID for the streaming delete operation

    println!("Initial insert LSN: {}", initial_insert_lsn);

    // The LSN passed to env.delete_row for a streaming op is used for the RawDeletionRecord,
    // but this LSN is typically overridden by the stream_commit_lsn when the transaction commits.
    // We use a distinct value here for clarity, but it's the stream_commit_lsn that's ultimately effective.
    let delete_op_event_lsn = 15;
    let stream_commit_lsn = 20; // LSN at which the streaming transaction (and its delete) is committed

    // --- Phase 1: Setup - Insert a row non-streamingly ---
    // This row (PK=1) will be added to pending writes.
    env.append_row(1, "Target User", 30, None).await;

    // Commit the non-streaming operation. This moves the row to the main mem_slice.
    // It is now "committed" at initial_insert_lsn but not yet flushed to disk.
    env.commit(initial_insert_lsn).await;

    // Inform the ReadStateManager that data up to initial_insert_lsn is committed.
    env.set_table_commit_lsn(initial_insert_lsn);
    // Set the replication LSN cap to allow reading up to initial_insert_lsn.
    env.set_replication_lsn(initial_insert_lsn);

    // Verify: The row (PK=1) should be visible in a snapshot at initial_insert_lsn.
    println!("1 Verifying snapshot at LSN {}", initial_insert_lsn);
    env.verify_snapshot(initial_insert_lsn, &[1]).await;

    // --- Phase 2: Action - Delete the non-streamed, unflushed row via a streaming transaction ---
    // Call delete_row for PK=1 within the context of stream_xact_id.
    // Inside table_handler.delete_in_stream_batch:
    //   - stream_state.mem_slice.delete() will be called. Since PK=1 was not added
    //     by stream_xact_id, it won't be in this transaction's mem_slice.
    //   - Thus, 'pos' returned by stream_state.mem_slice.delete() will be None.
    //   - The RawDeletionRecord will be created with pos: None.
    env.delete_row(
        1,
        "Target User",
        30,
        delete_op_event_lsn,
        Some(stream_xact_id),
    )
    .await;

    // Commit the streaming transaction.
    // During this commit, the TableHandler will process new_deletions for stream_xact_id.
    // For the deletion of PK=1 (which has pos: None), it should search the main mem_slice.
    // It will find PK=1 there and apply the deletion, associating it with stream_commit_lsn.
    env.stream_commit(stream_commit_lsn, stream_xact_id).await;

    // Update ReadStateManager: table state is now committed up to stream_commit_lsn.
    env.set_table_commit_lsn(stream_commit_lsn);
    // Update replication LSN cap to allow reading up to the new commit LSN.
    env.set_replication_lsn(stream_commit_lsn);

    // --- Phase 3: Verification ---
    // Verify: The row (PK=1) should NOT be visible in a snapshot at stream_commit_lsn.
    // The effective LSN for the read will be min(target_lsn=20, table_commit_lsn=20, replication_cap=20) = 20.
    println!("2 Verifying snapshot at LSN {}", stream_commit_lsn);
    env.verify_snapshot(stream_commit_lsn, &[]).await; // Expect empty slice (PK=1 deleted)

    env.shutdown().await;
    println!("Test test_stream_delete_unflushed_non_streamed_row passed!");
}
