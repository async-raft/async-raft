mod fixtures;

use std::sync::Arc;

use anyhow::Result;
use async_raft::Config;

use fixtures::{RaftRouter, sleep_for_a_sec};
use tracing::info;

const LEADER: u64 = 0;
const NON_VOTER: u64 = 1;
const REQUEST_COUNT_BEFORE_REMOVAL: u64 = 10;
const REQUEST_COUNT_AFTER_REMOVAL: u64 = 10;
const CLIENT_ID: &str = "client";
const CLUSTER_NAME: &str = "test";

/// Remove Non-Voter test.
///
/// Test plan:
///
///   1. Create a single-node cluster of Node 0.
///   1. Add Node 1 as a Non-Voter.
///   1. Send requests to Node 0.
///   1. Assert that Node 1 received the updates.
///   1. Remove Node 1.
///   1. Send requests to Node 0.
///   1. Assert that Node 1 DID NOT receive the updates.
///
/// RUST_LOG=async_raft,memstore,dynamic_membership=trace cargo test -p async-raft --test dynamic_membership
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn remove_non_voter() -> Result<()> {
    fixtures::init_tracing();

    let router = {
        info!("--- Setup test dependencies");

        let config = Arc::new(Config::build(CLUSTER_NAME.into()).validate().expect("failed to build Raft config"));
        Arc::new(RaftRouter::new(config))
    };

    {
        info!("--- Initializing and asserting on a single-node cluster");

        router.new_raft_node(LEADER).await;
        sleep_for_a_sec().await;
        router.assert_pristine_cluster().await;

        router.initialize_from_single_node(LEADER).await?;
        sleep_for_a_sec().await;
        router.assert_stable_cluster(Some(1), Some(1)).await;
    }

    {
        info!("--- Adding Node 1 as Non-Voter");

        router.new_raft_node(NON_VOTER).await;

        let add_non_voter_router = Arc::clone(&router);
        let non_voter_added = tokio::spawn(async move {
            let _ = add_non_voter_router.add_non_voter(LEADER, NON_VOTER).await;
        });

        let request_router = Arc::clone(&router);
        let request_processed = tokio::spawn(async move {
            sleep_for_a_sec().await;

            request_router.client_request(LEADER, CLIENT_ID, NON_VOTER).await;
        });

        tokio::join!(non_voter_added, request_processed).0?;
    }

    sleep_for_a_sec().await;

    {
        info!("--- Sending {} requests", REQUEST_COUNT_BEFORE_REMOVAL);

        router.client_request_many(LEADER, CLIENT_ID, REQUEST_COUNT_BEFORE_REMOVAL as usize).await;
    }

    sleep_for_a_sec().await;

    {
        info!("--- Asserting that the Non-Voter received the updates");

        let metrics = router
            .latest_metrics()
            .await
            .into_iter()
            .find(|m| m.id == NON_VOTER)
            .expect("expected to find the Non-Voter node");

        // +2 because
        //  1 - Initial entry by the leader.
        //  1 - Additional request to drive replication when adding the non-voter.
        assert_eq!(metrics.last_log_index, REQUEST_COUNT_BEFORE_REMOVAL + 2);
        assert_eq!(metrics.last_applied, REQUEST_COUNT_BEFORE_REMOVAL + 2);
    }

    {
        info!("--- Removing the Non-Voter Node 1");

        let _ = router.remove_non_voter(LEADER, NON_VOTER).await;
    }

    sleep_for_a_sec().await;

    {
        info!("--- Sending {} requests", REQUEST_COUNT_BEFORE_REMOVAL);

        router.client_request_many(LEADER, CLIENT_ID, REQUEST_COUNT_BEFORE_REMOVAL as usize).await;
    }

    sleep_for_a_sec().await;

    {
        info!("--- Asserting that the Leader received the updates");

        let metrics = router
            .latest_metrics()
            .await
            .into_iter()
            .find(|m| m.id == LEADER)
            .expect("expected to find the Leader node");

        assert_eq!(
            metrics.last_log_index,
            REQUEST_COUNT_BEFORE_REMOVAL + REQUEST_COUNT_AFTER_REMOVAL + 2
        );
        assert_eq!(
            metrics.last_applied,
            REQUEST_COUNT_BEFORE_REMOVAL + REQUEST_COUNT_AFTER_REMOVAL + 2
        );

        info!("--- Asserting that the Non-Voter did not receive the updates");

        let metrics = router
            .latest_metrics()
            .await
            .into_iter()
            .find(|m| m.id == NON_VOTER)
            .expect("expected to find the Non-Voter node");

        assert_eq!(metrics.last_log_index, REQUEST_COUNT_BEFORE_REMOVAL + 2);
        assert_eq!(metrics.last_applied, REQUEST_COUNT_BEFORE_REMOVAL + 2);
    }

    Ok(())
}
