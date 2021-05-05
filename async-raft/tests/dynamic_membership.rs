mod fixtures;

use std::sync::Arc;

use anyhow::Result;
use async_raft::Config;

use fixtures::{RaftRouter, sleep_for_a_sec};
use tracing::info;

const ORIGINAL_LEADER: u64 = 0;
const CLIENT_ID: &str = "client";
const CLUSTER_NAME: &str = "test";

/// Dynamic membership test.
///
/// Test plan:
///
///   1. Create a single-node cluster of Node 0.
///   1. Add a new nodes 1, 2, 3, 4 and assert that they've joined the cluster properly.
///   1. Propose a new config change where the old leader, Node 0 is not present, and assert that it steps down.
///   1. Temporarily isolate the new leader, and assert that an even newer leader takes over.
///   1. Restore the isolated node and assert that it becomes a follower.
///
/// RUST_LOG=async_raft,memstore,dynamic_membership=trace cargo test -p async-raft --test dynamic_membership
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn dynamic_membership() -> Result<()> {
    fixtures::init_tracing();

    let router = {
        info!("--- Setup test dependencies");

        let config = Arc::new(Config::build(CLUSTER_NAME.into()).validate().expect("failed to build Raft config"));
        Arc::new(RaftRouter::new(config))
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
        info!("--- Syncing nodes 1, 2, 3, 4");

        for node in 1u64..=4 {
            router.new_raft_node(node).await;
        }
    }

    {
        info!("--- Adding nodes 1, 2, 3, 4 as voters");

        for node in 1u64..=4 {
            info!("--- Adding node {}", node);
            router.new_raft_node(node).await;

            let add_voter_router = Arc::clone(&router);
            let voter_added = tokio::spawn(async move {
                let _ = add_voter_router.add_voter(ORIGINAL_LEADER, node).await;
            });

            let request_router = Arc::clone(&router);
            let request_processed = tokio::spawn(async move {
                sleep_for_a_sec().await;

                request_router.client_request(ORIGINAL_LEADER, CLIENT_ID, node).await;
            });

            tokio::join!(voter_added, request_processed).0?;
        }
    }

    sleep_for_a_sec().await;

    {
        info!("--- Asserting on the new cluster configuration");

        router.assert_stable_cluster(
            Some(1),
            Some(9),
        ).await;
    }

    {
        info!("--- Isolating original leader Node 0");

        router.isolate_node(ORIGINAL_LEADER).await;
    }

    // Wait for election and for everything to stabilize (this is way longer than needed).
    sleep_for_a_sec().await;

    let new_leader = {
        info!("--- Asserting that a new leader took over");

        router.assert_stable_cluster(
            Some(2),
            Some(10)
        ).await;
        let new_leader = router.leader().await.expect("expected new leader");
        assert_ne!(new_leader, ORIGINAL_LEADER, "expected new leader to be different from the old leader");

        new_leader
    };

    {
        info!("--- Restoring isolated old leader Node 0");

        router.restore_node(ORIGINAL_LEADER).await;
    }

    sleep_for_a_sec().await;

    {
        info!("--- Asserting that the leader of the cluster stayed the same");

        // We should still be in term 2, as leaders should not be deposed when
        // they are not missing heartbeats.
        router.assert_stable_cluster(
            Some(2),
            Some(10)
        ).await;
        let current_leader = router.leader().await.expect("expected to find current leader");
        assert_eq!(new_leader, current_leader, "expected cluster leadership to stay the same");
    }

    Ok(())
}
