use std::sync::Arc;
use std::time::Duration;
use std::collections::HashSet;

use anyhow::Result;
use futures::stream::StreamExt;
use maplit::hashset;

use async_raft::Config;
use async_raft::State;
use fixtures::RaftRouter;

mod fixtures;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn remove_voter_non_voter() -> Result<()> {
    fixtures::init_tracing();

    let timeout = Duration::from_millis(500);
    let all_members = hashset![0,1,2,3,4];

    // Setup test dependencies.
    let config = Arc::new(Config::build("test".into()).validate().expect("failed to build Raft config"));
    let router = Arc::new(RaftRouter::new(config.clone()));
    router.new_raft_node(0).await;

    // Assert all nodes are in non-voter state & have no entries.
    let mut want = 0;
    router.wait_for_metrics(&0u64, |x| { x.last_log_index == want }, timeout, &format!("n{}.last_log_index -> {}", 0, 0)).await;
    router.wait_for_metrics(&0u64, |x| { x.state == State::NonVoter }, timeout, &format!("n{}.state -> {:?}", 4, State::NonVoter)).await;

    router.assert_pristine_cluster().await;

    // Initialize the cluster, then assert that a stable cluster was formed & held.
    tracing::info!("--- initializing cluster");
    router.initialize_from_single_node(0).await?;
    want = 1;
    wait_log(router.clone(), &hashset![0], want).await;

    router.assert_stable_cluster(Some(1), Some(1)).await;

    // Sync some new nodes.
    router.new_raft_node(1).await;
    router.new_raft_node(2).await;
    router.new_raft_node(3).await;
    router.new_raft_node(4).await;
    tracing::info!("--- adding new nodes to cluster");
    let mut new_nodes = futures::stream::FuturesUnordered::new();
    new_nodes.push(router.add_non_voter(0, 1));
    new_nodes.push(router.add_non_voter(0, 2));
    new_nodes.push(router.add_non_voter(0, 3));
    new_nodes.push(router.add_non_voter(0, 4));
    while let Some(inner) = new_nodes.next().await {
        inner?;
    }

    // TODO: This does not pass:
    // Leader update last_log_index to 1.
    // Followers have last_log_index 0.
    // wait_log(router.clone(), &all_members, 1).await;


    tracing::info!("--- changing cluster config");
    router.change_membership(0, hashset![0, 1, 2, 3, 4]).await?;

    // one initial log and 2 member-change logs
    want = 3;
    wait_log(router.clone(), &all_members, want).await;

    router.assert_stable_cluster(Some(1), Some(3)).await; // Still in term 1, so leader is still node 0.

    // Send some requests
    router.client_request_many(0, "client", 100).await;

    want += 100;
    wait_log(router.clone(), &all_members, want).await;

    // Remove Node 4
    tracing::info!("--- remove n{}", 4);
    router.change_membership(0, hashset![0, 1, 2, 3]).await?;

    // two member-change logs
    want += 2;
    wait_log(router.clone(), &all_members, want).await;

    router.wait_for_metrics(&4u64, |x| { x.state == State::NonVoter }, timeout, &format!("n{}.state -> {:?}", 4, State::NonVoter)).await;

    // Send some requests
    router.client_request_many(0, "client", 100).await;

    // NOTE: This should not pass:
    // Logs are still sent to node 4, which is removed from the cluster.
    want += 100;
    wait_log(router.clone(), &all_members, want).await;

    Ok(())
}

async fn wait_log(router: std::sync::Arc<fixtures::RaftRouter>, node_ids: &HashSet<u64>, want_log: u64) {
    let timeout = Duration::from_millis(500);
    for i in node_ids.iter() {
        router.wait_for_metrics(&i, |x| { x.last_log_index == want_log }, timeout, &format!("n{}.last_log_index -> {}", i, want_log)).await;
        router.wait_for_metrics(&i, |x| { x.last_applied == want_log }, timeout, &format!("n{}.last_applied -> {}", i, want_log)).await;
    }
}
