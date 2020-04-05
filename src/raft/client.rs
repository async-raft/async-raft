use actix::prelude::*;
use log::{error};
use futures::channel::oneshot;
use futures::future::ready;

use crate::{
    AppData, AppDataResponse, AppError,
    common::{CLIENT_RPC_RX_ERR, CLIENT_RPC_TX_ERR, ApplyLogsTask, ClientPayloadWithChan, DependencyAddr},
    network::RaftNetwork,
    messages::{ClientError, ClientPayload, ClientPayloadResponse, ResponseMode},
    raft::{RaftState, Raft},
    replication::RSReplicate,
    storage::{AppendEntryToLog, RaftStorage},
    try_fut::TryActorFutureExt,
};

impl<D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D>, S: RaftStorage<D, R, E>> Handler<ClientPayload<D, R, E>> for Raft<D, R, E, N, S> {
    type Result = ResponseActFuture<Self, Result<ClientPayloadResponse<R>, ClientError<D, R, E>>>;

    /// Handle client requests.
    fn handle(&mut self, msg: ClientPayload<D, R, E>, _: &mut Self::Context) -> Self::Result {
        // Queue the message for processing or forward it along to the leader.
        let response_chan = match &mut self.state {
            RaftState::Leader(state) => {
                // Wrap the given message for async processing.
                let (tx, rx) = oneshot::channel();
                let with_chan = ClientPayloadWithChan{tx, rpc: msg};
                let _ = state.client_request_queue.unbounded_send(with_chan).map_err(|_| {
                    error!("Unexpected error while queueing client request for processing.")
                });
                rx
            },
            _ => {
                return Box::new(fut::err(ClientError::ForwardToLeader{payload: msg, leader: self.current_leader}));
            },
        };

        // Build a response from the message's channel.
        Box::new(fut::wrap_future(response_chan)
            .map_err(|e, _: &mut Self, _| {
                error!("{} {:?}", CLIENT_RPC_RX_ERR, e);
                ClientError::Internal
            })
            .and_then(|res, _, _| fut::result(res)))
    }
}

impl<D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D>, S: RaftStorage<D, R, E>> Raft<D, R, E, N, S> {
    /// Process the given client RPC, appending it to the log and committing it to the cluster.
    ///
    /// This function takes the given RPC, appends its entries to the log, sends the entries out
    /// to the replication streams to be replicated to the cluster followers, after half of the
    /// cluster members have successfully replicated the entries this routine will proceed with
    /// applying the entries to the state machine. Then the next RPC is processed.
    pub(super) fn process_client_rpc(&mut self, _: &mut Context<Self>, msg: ClientPayloadWithChan<D, R, E>) -> Box<dyn ActorFuture<Actor=Self, Output=()>> {
        match &self.state {
            // If node is still leader, continue.
            RaftState::Leader(_) => (),
            // If node is in any other state, then forward the message to the leader.
            _ => {
                let _ = msg.tx.send(Err(ClientError::ForwardToLeader{payload: msg.rpc, leader: self.current_leader}))
                    .map_err(|_| error!("{} Error while forwarding to leader at the start of process_client_rpc.", CLIENT_RPC_TX_ERR));
                return Box::new(ready(()).into_actor(self));
            }
        };

        // Assign an index to the payload and prep it for storage & replication.
        let payload = msg.upgrade(self.last_log_index + 1, self.current_term);

        // Send the payload over to the storage engine.
        self.is_appending_logs = true; // NOTE: this routine is pipelined, but we still use a semaphore in case of transition to follower.
        Box::new(fut::wrap_future(self.storage.send::<AppendEntryToLog<D, E>>(AppendEntryToLog::new(payload.entry())))
            .map_err(|err, act: &mut Self, ctx| {
                act.map_fatal_actix_messaging_error(ctx, err, DependencyAddr::RaftStorage);
                ClientError::Internal
            })
            .and_then(|res, _, _| fut::result(res.map_err(|err| ClientError::Application(err))))

            // Handle results from storage engine.
            .then(move |res, act, _| {
                act.is_appending_logs = false;
                match res {
                    Ok(_) => {
                        act.last_log_index = payload.index;
                        act.last_log_term = act.current_term;
                        fut::result(Ok(payload))
                    }
                    Err(err) => {
                        error!("Node {} received an error from the storage engine.", &act.id);
                        let _ = payload.tx.send(Err(err)).map_err(|err| error!("{} {:?}", CLIENT_RPC_RX_ERR, err));
                        fut::err(())
                    }
                }
            })

            // Send logs over for replication.
            .and_then(move |payload, act, _| {
                let state = match &mut act.state {
                    RaftState::Leader(state) => state,
                    _ => {
                        let msg = payload.downgrade();
                        let _ = msg.tx.send(Err(ClientError::ForwardToLeader{payload: msg.rpc, leader: act.current_leader}))
                            .map_err(|_| error!("{} Error while forwarding to leader at the end of process_client_rpc.", CLIENT_RPC_RX_ERR));
                        return fut::ok(());
                    }
                };

                // If there are peer voting members to replicate to, then setup the
                // request to await being comitted to the cluster & send payload over to
                // each replication stream as needed.
                let nodeid = &act.id;
                let voting_peer_count = act.membership.members.iter().filter(|e| *e != nodeid).count();
                if voting_peer_count > 0 {
                    let entry = payload.entry();
                    state.awaiting_committed.push(payload);
                    for rs in state.nodes.values() {
                        let _ = rs.addr.do_send(RSReplicate{entry: entry.clone(), line_commit: act.commit_index});
                    }
                } else {
                    // If there are any non-voting members, replicate to them.
                    if act.membership.non_voters.len() > 0 {
                        let entry = payload.entry();
                        for rs in state.nodes.values() {
                            let _ = rs.addr.do_send(RSReplicate{entry: entry.clone(), line_commit: act.commit_index});
                        }
                    }

                    // The payload is committed. Send it over to be applied to state machine.
                    act.commit_index = payload.index;
                    if let &ResponseMode::Committed = &payload.response_mode {
                        // If this RPC is configured to wait only for log committed, then respond to client now.
                        let entry = payload.entry();
                        let _ = payload.tx.send(Ok(ClientPayloadResponse::Committed{index: payload.index})).map_err(|err| error!("{} {:?}", CLIENT_RPC_RX_ERR, err));
                        let _ = act.apply_logs_pipeline.unbounded_send(ApplyLogsTask::Entry{entry, chan: None});
                    } else {
                        // Else, send it through the pipeline and it will be responded to afterwords.
                        let _ = act.apply_logs_pipeline.unbounded_send(ApplyLogsTask::Entry{entry: payload.entry(), chan: Some(payload.tx)});
                    }
                }
                fut::ok(())
            })
            .map(|_, _, _| ())
        )
    }
}
