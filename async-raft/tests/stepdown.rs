mod fixtures;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_raft::{Config, State};
use maplit::hashset;
use tokio::time::sleep;

use fixtures::RaftRouter;

/// Leader stepdown test.
///
/// Test plan:
///
/// - Create a single-node cluster of Node 0.
/// - Add three new nodes (1, 2, 3) as Voters.
/// - Ask the leader (Node 0) to remove itself from the cluster.
/// - Ensure that the old leader (Node 0) no longer gets updates.
///
/// RUST_LOG=async_raft,memstore,stepdown=trace cargo test -p async-raft --test stepdown
#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn stepdown() -> Result<()> {
    fixtures::init_tracing();

    // Setup test dependencies.
    let config = Arc::new(Config::build("test".into()).validate().expect("failed to build Raft config"));
    let router = Arc::new(RaftRouter::new(config.clone()));
    router.new_raft_node(0).await;

    // Assert all nodes are in non-voter state & have no entries.
    sleep(Duration::from_secs(1)).await;
    router.assert_pristine_cluster().await;

    // Initialize the cluster, then assert that a stable cluster was formed & held.
    tracing::info!("--- Initializing cluster");
    router.initialize_from_single_node(0).await?;
    sleep(Duration::from_secs(1)).await;
    router.assert_stable_cluster(Some(1), Some(1)).await;

    // Add three new nodes to the cluster.
    let original_leader = router.leader().await.expect("expected the cluster to have a leader");
    assert_eq!(0, original_leader, "expected original leader to be node 0");
    for node in 1u64..=3 {
        tracing::info!("--- Adding node {}", node);
        router.new_raft_node(node).await;

        let add_voter_router = Arc::clone(&router);
        let voter_added = tokio::spawn(async move {
            let _ = add_voter_router.add_voter(original_leader, node).await;
        });

        let request_router = Arc::clone(&router);
        let request_processed = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;

            request_router.client_request(original_leader, "client", node).await;
        });

        tokio::join!(voter_added, request_processed).0?;
    }

    // Give time for step down metrics to flow through.
    sleep(Duration::from_secs(1)).await;

    // Assert on the state of the old leader.
    {
        let metrics = router
            .latest_metrics()
            .await
            .into_iter()
            .find(|node| node.id == original_leader)
            .expect("expected to find metrics on original leader node");
        let cfg = metrics.membership_config;
        assert_eq!(metrics.state, State::Leader, "expected node 0 to be the old leader");
        assert_eq!(
            metrics.current_term, 1,
            "expected old leader to still be in first term, got {}",
            metrics.current_term
        );
        // 7 because
        //   1 - initial entry
        //   2, 3 - add node 1 and a request
        //   4, 5 - add node 2 and a request
        //   6, 7 - add node 3 and a request
        assert_eq!(
            metrics.last_log_index, 7,
            "expected old leader to have last log index of 7, got {}",
            metrics.last_log_index
        );
        assert_eq!(
            metrics.last_applied, 7,
            "expected old leader to have last applied of 7, got {}",
            metrics.last_applied
        );
        assert_eq!(
            cfg.members,
            hashset![0, 1, 2, 3],
            "expected old leader to have membership of [0, 1, 2, 3], got {:?}",
            cfg.members
        );
    }

    // Perform the actual stepdown.
    {
        tracing::info!("--- Old leader stepping down");
        let remove_leader_router = Arc::clone(&router);
        let leader_removed = tokio::spawn(async move {
            let _ = remove_leader_router.remove_voter(original_leader, original_leader).await;
        });

        let request_router = Arc::clone(&router);
        let request_processed = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let new_leader = request_router
                .latest_metrics()
                .await
                .into_iter()
                .find(|m| m.state == State::Leader)
                .expect("expected the cluster to have a new leader")
                .id;

            request_router.client_request(new_leader, "client", 0).await;
        });

        tokio::join!(leader_removed, request_processed).0?;
    }

    // Give time for step down metrics to flow through.
    sleep(Duration::from_secs(1)).await;

    // Assert on the state of the cluster without the old leader.
    {
        let metrics = router
            .latest_metrics()
            .await
            .into_iter()
            .find(|m| m.state == State::Leader)
            .expect("expected the cluster to have a new leader");

        let cfg = metrics.membership_config;
        assert_eq!(
            metrics.current_term, 2,
            "expected the new leader to be in term 2, got {}",
            metrics.current_term
        );
        // 10 because
        //   we carried over 7 from before
        //   8 - node removal
        //   9 - request
        //   10 - the new leader starts its term with a new entry
        assert_eq!(
            metrics.last_log_index, 10,
            "expected the new leader to have last log index of 10, got {}",
            metrics.last_log_index
        );
        assert_eq!(
            metrics.last_applied, 10,
            "expected the new leader to have last applied of 10, got {}",
            metrics.last_applied
        );
        assert_eq!(
            cfg.members,
            hashset![1, 2, 3],
            "expected new cluster to have membership of [1, 2, 3], got {:?}",
            cfg.members
        );
    }

    // Assert that the old leader no longer gets updates.
    {
        let new_leader_metrics = router
            .latest_metrics()
            .await
            .into_iter()
            .find(|m| m.state == State::Leader)
            .expect("expected the cluster to have a new leader");

        let old_leader_metrics = router
            .latest_metrics()
            .await
            .into_iter()
            .find(|m| m.id == original_leader)
            .expect("expected the cluster to have a new leader");

        assert!(
            old_leader_metrics.current_term < new_leader_metrics.current_term,
            "expected the old leader to have term less than that of the new leader"
        );

        assert!(
            old_leader_metrics.last_log_index < new_leader_metrics.last_log_index,
            "expected the old leader to have last log index less than that of the new leader"
        );
    }

    Ok(())
}
