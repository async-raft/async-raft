//! Test cluster initialization.

mod fixtures;

use std::time::Duration;

use actix::prelude::*;
use actix_raft::{
    admin::{InitWithConfig, InitWithConfigError},
    messages::{ClientError, EntryNormal, ResponseMode},
    metrics::{State},
    try_fut::{TryActorFutureExt, TryActorStreamExt},
};
use futures::future::{try_join_all, TryFutureExt, FutureExt};
use log::{error};
use tokio::time::delay_for;

use fixtures::{
    Payload, RaftTestController, Node, setup_logger,
    dev::{ExecuteInRaftRouter, GetCurrentLeader, RaftRouter, Register},
    memory_storage::MemoryStorageData,
};

/// Cluster initialization test.
///
/// What does this test cover?
///
/// - Bring 3 nodes online with only knowledge of themselves.
/// - Issue the `InitWithConfig` command to all nodes.
/// - Assert that the cluster was able to come online, elect a leader and maintain a stable state.
///
/// `RUST_LOG=actix_raft,initialization=debug cargo test initialization`
#[test]
fn initialization() {
    setup_logger();
    let sys = System::builder().stop_on_panic(true).name("test").build();

    // Setup test dependencies.
    let net = RaftRouter::new();
    let network = net.start();
    let node0 = Node::builder(0, network.clone(), vec![0]).build();
    network.do_send(Register{id: 0, addr: node0.addr.clone()});
    let node1 = Node::builder(1, network.clone(), vec![1]).build();
    network.do_send(Register{id: 1, addr: node1.addr.clone()});
    let node2 = Node::builder(2, network.clone(), vec![2]).build();
    network.do_send(Register{id: 2, addr: node2.addr.clone()});

    // Setup test controller and actions.
    let mut ctl = RaftTestController::new(network);
    ctl.register(0, node0.addr.clone()).register(1, node1.addr.clone()).register(2, node2.addr.clone());
    ctl.start_with_test(10, Box::new(|act, ctx| {
        // Assert that all nodes are in NonVoter state with index 0.
        let task = fut::wrap_future(act.network.send(ExecuteInRaftRouter(Box::new(move |act, _| {
            for node in act.metrics.values() {
                assert!(node.state == State::NonVoter, "Expected all nodes to be in NonVoter state at beginning of test.");
                assert_eq!(node.last_log_index, 0, "Expected all nodes to be at index 0 at beginning of test.");
            }
        }))))
            .map_err(|err, _: &mut RaftTestController, _| panic!(err)).and_then(|res, _, _| fut::result(res))

            // Issue the InitWithConfig command to all nodes in parallel.
            .and_then(|_, act, _| {
                let mut cmds: Vec<Box<dyn Future<Output=Result<(), InitWithConfigError>> + Unpin>> = vec![];
                cmds.push(Box::new(act.nodes.get(&0).expect("Expected node 0 to be registered with test controller.")
                    .send(InitWithConfig::new(vec![0, 1, 2])).map(Result::unwrap)));
                cmds.push(Box::new(act.nodes.get(&1).expect("Expected node 1 to be registered with test controller.")
                    .send(InitWithConfig::new(vec![0, 1, 2])).map(Result::unwrap)));
                cmds.push(Box::new(act.nodes.get(&2).expect("Expected node 2 to be registered with test controller.")
                    .send(InitWithConfig::new(vec![0, 1, 2])).map(Result::unwrap)));

                fut::wrap_future(try_join_all(cmds).map_err(|err| panic!(err)))
            })

            // Delay for a bit and then write data to the cluster.
            .and_then(|_, _, _| fut::wrap_future(delay_for(Duration::from_secs(5)).map(Ok)))
            .and_then(|_, act, ctx| act.write_data(ctx))

            // Assert against the state of the cluster.
            .and_then(|_, _, _| fut::wrap_future(delay_for(Duration::from_secs(2)).map(Ok)))
            .and_then(|_, act, _| {
                fut::wrap_future(act.network.send(ExecuteInRaftRouter(Box::new(move |act, _| {
                    // Cluster should have an elected leader.
                    let leader = act.metrics.values().find(|e| &e.state == &State::Leader)
                        .expect("Expected cluster to have an elected leader.");

                    // All nodes should agree upon the leader, term, index, applied & config.
                    assert!(act.metrics.values().all(|node| node.current_leader == Some(leader.id)), "Expected all nodes to agree upon current leader.");
                    assert!(act.metrics.values().all(|node| node.current_term == leader.current_term), "Expected all nodes to agree upon current term.");
                    assert!(act.metrics.values().all(|node| node.last_log_index == leader.last_log_index), "Expected all nodes to be at the same index.");
                    assert!(act.metrics.values().all(|node| node.membership_config == leader.membership_config), "Expected all nodes to have same cluster config.");

                    System::current().stop();
                }))))
                .map_err(|err, _, _| panic!(err)).and_then(|res, _, _| fut::result(res))
            })

            .map_err(|err, _, _| panic!("Failure during test. {:?}", err))
            .or_default();

        ctx.spawn(task);
    }));

    // Run the test.
    assert!(sys.run().is_ok(), "Error during test.");
}

impl RaftTestController {
    fn write_data(&mut self, _: &mut Context<Self>) -> impl ActorFuture<Actor=Self, Output=Result<(), ()>> {
        fut::wrap_future(self.network.send(GetCurrentLeader))
            .map_err(|_, _: &mut Self, _| panic!("Failed to get current leader."))
            .and_then(|res, _, _| fut::result(res))
            .and_then(|current_leader, act, _| {
                let num_requests = 100;
                let leader_id = current_leader.expect("Expected to find a current cluster leader for writing client requests.");
                let addr = act.nodes.get(&leader_id).expect("Expected leader to be present it RaftTestController's nodes map.");
                let leader = addr.clone();

                fut::wrap_stream(futures::stream::iter(0..num_requests))
                    .then(move |data, _, _| {
                        let entry = EntryNormal{data: MemoryStorageData{data: data.to_string().into_bytes()}};
                        let payload = Payload::new(entry, ResponseMode::Applied);
                        fut::wrap_future(leader.clone().send(payload))
                            .map_err(|_, _, _| ClientError::Internal)
                            .and_then(|res, _, _| fut::result(res))
                            .then(move |res, _, _| match res {
                                Ok(_) => fut::ok(()),
                                Err(err) => {
                                    error!("TEST: Error during client request. {}", err);
                                    fut::err(())
                                }
                            })
                    })
                    .try_finish()
            })
    }
}
