use std::sync::Arc;

use actix::prelude::*;
use futures::channel::oneshot;
use futures::future::ready;
use log::{error};

use crate::{
    AppData, AppDataResponse, AppError,
    common::{CLIENT_RPC_TX_ERR, ApplyLogsTask, DependencyAddr},
    messages::{ClientPayloadResponse, ClientError, Entry},
    network::RaftNetwork,
    raft::Raft,
    storage::{ApplyEntryToStateMachine, ReplicateToStateMachine, GetLogEntries, RaftStorage},
    try_fut::TryActorFutureExt,
};

impl<D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D>, S: RaftStorage<D, R, E>> Raft<D, R, E, N, S> {
    /// Process tasks for applying logs to the state machine.
    ///
    /// **NOTE WELL:** these operations are strictly pipelined to ensure that these operations
    /// happen in strict order for guaranteed linearizability.
    pub(super) fn process_apply_logs_task(&mut self, ctx: &mut Context<Self>, msg: ApplyLogsTask<D, R, E>) -> Box<dyn ActorFuture<Actor=Self, Output=()>> {
        match msg {
            ApplyLogsTask::Entry{entry, chan} => self.process_apply_logs_task_with_entries(ctx, entry, chan),
            ApplyLogsTask::Outstanding => self.process_apply_logs_task_outstanding(ctx),
        }
    }

    /// Apply the given payload of log entries to the state machine.
    fn process_apply_logs_task_with_entries(
        &mut self, _: &mut Context<Self>, entry: Arc<Entry<D>>, chan: Option<oneshot::Sender<Result<ClientPayloadResponse<R>, ClientError<D, R, E>>>>,
    ) -> Box<dyn ActorFuture<Actor=Self, Output=()>> {
        // PREVIOUS TERM UNCOMMITTED LOGS CHECK:
        // Here we are checking to see if there are any logs from the previous term which were
        // outstanding, but which are now safe to apply due to being covered by a new definitive
        // commit index from this term.
        let entry_index = entry.index;
        let f = if (self.last_applied + 1) != entry_index {
            fut::Either::Left(fut::wrap_future(self.storage.send::<GetLogEntries<D, E>>(GetLogEntries::new(self.last_applied + 1, entry_index)))
                .map_err(|err, act: &mut Self, ctx| {
                    act.map_fatal_actix_messaging_error(ctx, err, DependencyAddr::RaftStorage);
                    ClientError::Internal
                })
                .and_then(|res, act, ctx| act.map_fatal_storage_result(ctx, res).map_err(|_, _, _| ClientError::Internal))
                .and_then(|res, act, _| {
                    let line_index = res.iter().last().map(|e| e.index);
                    fut::wrap_future(act.storage.send::<ReplicateToStateMachine<D, E>>(ReplicateToStateMachine::new(res)))
                        .map_err(|err, act: &mut Self, ctx| {
                            act.map_fatal_actix_messaging_error(ctx, err, DependencyAddr::RaftStorage);
                            ClientError::Internal
                        })
                        .and_then(|res, act, ctx| act.map_fatal_storage_result(ctx, res).map_err(|_, _, _| ClientError::Internal))
                        .and_then(move |_, act, _| {
                            // Update state after a success operation on the state machine.
                            if let Some(index) = line_index {
                                act.last_applied = index;
                            }
                            fut::ok(())
                        })
                }))
        } else {
            let res: Result<(), ClientError<D, R, E>> = Ok(());
            fut::Either::Right(fut::result(res))
        };

        // Resume with the normal flow of logic. Here we are simply taking the payload of entries
        // to be applied to the state machine, applying and then responding as needed.
        let line_index = entry.index;
        Box::new(f.and_then(move |_, act, _| {
            fut::wrap_future(act.storage.send::<ApplyEntryToStateMachine<D, R, E>>(ApplyEntryToStateMachine::new(entry)))
                .map_err(|err, act: &mut Self, ctx| {
                    act.map_fatal_actix_messaging_error(ctx, err, DependencyAddr::RaftStorage);
                    ClientError::Internal
                })
                .and_then(|res, act, ctx| act.map_fatal_storage_result(ctx, res)
                    .map_err(|_, _, _| ClientError::Internal))
        })

            .map(move |res, act, _| match res {
                Ok(data) => {
                    // Update state after a success operation on the state machine.
                    act.last_applied = line_index;

                    if let Some(tx) = chan {
                        let _ = tx.send(Ok(ClientPayloadResponse::Applied{index: line_index, data})).map_err(|err| error!("{} {:?}", CLIENT_RPC_TX_ERR, err));
                    }
                }
                Err(err) => {
                    if let Some(tx) = chan {
                        let _ = tx.send(Err(err)).map_err(|err| error!("{} {:?}", CLIENT_RPC_TX_ERR, err));
                    }
                }
            }))
    }

    /// Check for any outstanding logs which have been committed but which have not been applied,
    /// then apply them.
    fn process_apply_logs_task_outstanding(&mut self, _: &mut Context<Self>) -> Box<dyn ActorFuture<Actor=Self, Output=()>> {
        // Guard against no-op.
        if &self.last_applied == &self.commit_index {
            return Box::new(ready(()).into_actor(self));
        }

        // Fetch the series of entries which must be applied to the state machine.
        let start = self.last_applied + 1;
        let stop = self.commit_index + 1;
        Box::new(fut::wrap_future(self.storage.send::<GetLogEntries<D, E>>(GetLogEntries::new(start, stop)))
            .map_err(|err, act: &mut Self, ctx| act.map_fatal_actix_messaging_error(ctx, err, DependencyAddr::RaftStorage))
            .and_then(|res, act, ctx| act.map_fatal_storage_result(ctx, res))

            // Send the entries over to the storage engine to be applied to the state machine.
            .and_then(|entries, act, _| {
                let line_index = entries.last().map(|elem| elem.index);
                fut::wrap_future(act.storage.send::<ReplicateToStateMachine<D, E>>(ReplicateToStateMachine::new(entries)))
                    .map_err(|err, act: &mut Self, ctx| act.map_fatal_actix_messaging_error(ctx, err, DependencyAddr::RaftStorage))
                    .and_then(|res, act, ctx| act.map_fatal_storage_result(ctx, res))
                    .map_ok(move |_, _, _| line_index)
            })

            // Update self to reflect progress on applying logs to the state machine.
            .and_then(move |line_index, act, _| {
                if let Some(index) = line_index {
                    act.last_applied = index;
                }
                fut::ok(())
            }).map(|_, _, _| ()))
    }
}
