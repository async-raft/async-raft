mod fixtures;

use std::sync::Arc;

use anyhow::Result;
use async_raft::raft::MembershipConfig;
use async_raft::{Config, SnapshotPolicy};
use maplit::hashset;

use fixtures::{RaftRouter, sleep_for_a_sec};
use tracing::info;

const ENTRIES_BETWEEN_SNAPSHOTS_LIMIT: u64 = 500;
const ORIGINAL_LEADER: u64 = 0;
const ADDED_FOLLOWER: u64 = 1;
const CLIENT_ID: &str = "client";

/// Compaction test.
///
/// Test plan:
///
///   1. Create a single-node cluster of Node 0.
///   1. Send enough requests to the node that log compaction will be triggered.
///   1. Add a new node (Node 0), and assert that it received the snapshot.
///
/// RUST_LOG=async_raft,memstore,compaction=trace cargo test -p async-raft --test compaction
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction() -> Result<()> {
    fixtures::init_tracing();

    let router = {
        info!("--- Setup test dependencies");

        let config = Arc::new(
            Config::build("test".into())
                .snapshot_policy(SnapshotPolicy::LogsSinceLast(ENTRIES_BETWEEN_SNAPSHOTS_LIMIT))
                .validate()
                .expect("failed to build Raft config"),
        );
        Arc::new(RaftRouter::new(config.clone()))
    };

    {
        info!("--- Initializing and asserting on a single-node cluster");

        router.new_raft_node(ORIGINAL_LEADER).await;
        sleep_for_a_sec().await;
        router.assert_pristine_cluster().await;

        router.initialize_from_single_node(ORIGINAL_LEADER).await?;
        sleep_for_a_sec().await;
        router.assert_stable_cluster(Some(1), Some(1)).await;
    }

    {
        info!("--- Triggering compaction");

        // Send enough requests to the cluster that compaction on the node should be triggered.
        // On pristine startup, we always put a single entry into the log. Thus, adding
        // LIMIT - 1 entries more to the log exactly triggers compaction.
        router.client_request_many(
            ORIGINAL_LEADER,
            CLIENT_ID,
            (ENTRIES_BETWEEN_SNAPSHOTS_LIMIT - 1) as usize
        ).await;
    }

    // Wait to ensure there is enough time for a snapshot to be built.
    sleep_for_a_sec().await;

    {
        info!("--- Asserting the creation of the snapshot");

        router.assert_stable_cluster(Some(1), Some(ENTRIES_BETWEEN_SNAPSHOTS_LIMIT)).await;
        router.assert_storage_state(
            1,
            ENTRIES_BETWEEN_SNAPSHOTS_LIMIT,
            Some(0),
            ENTRIES_BETWEEN_SNAPSHOTS_LIMIT,
            Some((
                ENTRIES_BETWEEN_SNAPSHOTS_LIMIT.into(),
                1,
                MembershipConfig {
                    members: hashset![0],
                },
            )),
        )
        .await;
    }

    sleep_for_a_sec().await;

    {
        info!("--- Adding new node to the cluster");

        router.new_raft_node(ADDED_FOLLOWER).await;
        router.add_non_voter(ORIGINAL_LEADER, ADDED_FOLLOWER).await.expect("failed to add new node as non-voter");
        let add_voter_router = Arc::clone(&router);
        let voter_added = tokio::spawn(async move {
            let _ = add_voter_router.add_voter(ORIGINAL_LEADER, ADDED_FOLLOWER).await;
        });

        let request_router = Arc::clone(&router);
        let request_processed = tokio::spawn(async move {
            sleep_for_a_sec().await;

            request_router.client_request(ORIGINAL_LEADER, CLIENT_ID, 1).await;
        });

        tokio::join!(voter_added, request_processed).0?;
    }

    sleep_for_a_sec().await;

    {
        info!("--- Asserting whether the follower received the snapshot");

        // +2 because
        //   1 - config change
        //   2 - additional request
        router.assert_stable_cluster(
            Some(1),
            Some(ENTRIES_BETWEEN_SNAPSHOTS_LIMIT + 2)
        ).await;

        let expected_snapshot = Some((
            ENTRIES_BETWEEN_SNAPSHOTS_LIMIT.into(),
            1,
            MembershipConfig {
                members: hashset![0u64],
            },
        ));

        router.assert_storage_state(
            1,
            ENTRIES_BETWEEN_SNAPSHOTS_LIMIT + 2,
            None, // This value is None because non-voters do not vote.
            ENTRIES_BETWEEN_SNAPSHOTS_LIMIT + 2,
            expected_snapshot
        ).await;
    }

    Ok(())
}
