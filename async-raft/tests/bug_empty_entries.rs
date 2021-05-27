use std::sync::Arc;
use std::time::Duration;
use std::collections::HashSet;

use anyhow::Result;
use maplit::hashset;

use async_raft::Config;
use fixtures::RaftRouter;

mod fixtures;

/// Cluster bug_empty_entries test.
///
///
///
/// What does this test do?
///
/// - brings a one node cluster online.
/// - write a log.
/// - brings a non-voter, setup replication
/// - it shows that the log will never be replicated to the non-voter.
///
/// RUST_LOG=async_raft,memstore,bug_empty_entries=trace cargo test -p async-raft --test bug_empty_entries
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn bug_empty_entries() -> Result<()> {
    fixtures::init_tracing();

    // Setup test dependencies.
    let config = Arc::new(Config::build("test".into()).validate().expect("failed to build Raft config"));
    let router = Arc::new(RaftRouter::new(config.clone()));

    router.new_raft_node(0).await;

    let mut want;

    tracing::info!("--- initializing cluster of 1 node");
    router.initialize_from_single_node(0).await?;
    want = 1;
    wait_log(router.clone(), &hashset![0], want).await?;


    tracing::info!("--- write one log");
    router.client_request_many(0, "client", 1).await;
    want += 1;
    wait_log(router.clone(), &hashset![0], want).await?;


    tracing::info!("--- adding 1 non-voter nodes");
    router.new_raft_node(1).await;
    router.add_non_voter(0, 1).await?;
    wait_log(router.clone(), &hashset![0, 1], want).await?;

    Ok(())
}

async fn wait_log(router: std::sync::Arc<fixtures::RaftRouter>, node_ids: &HashSet<u64>, want_log: u64) -> anyhow::Result<()> {
    let timeout = Duration::from_millis(500);
    for i in node_ids.iter() {
        router.wait_for_metrics(&i, |x| { x.last_log_index == want_log }, timeout, &format!("n{}.last_log_index -> {}", i, want_log)).await?;
        router.wait_for_metrics(&i, |x| { x.last_applied == want_log }, timeout, &format!("n{}.last_applied -> {}", i, want_log)).await?;
    }
    Ok(())
}
