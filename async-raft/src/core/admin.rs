use std::collections::HashSet;

use futures::future::{FutureExt, TryFutureExt};
use tokio::sync::oneshot;

use crate::core::client::ClientRequestEntry;
use crate::core::{ConsensusState, LeaderState, NonVoterReplicationState, NonVoterState, State, UpdateCurrentLeader, ConfigChangeOperation};
use crate::error::{ChangeConfigError, InitializeError, RaftError};
use crate::raft::{ChangeMembershipTx, ClientWriteRequest, MembershipConfig};
use crate::replication::RaftEvent;
use crate::{AppData, AppDataResponse, NodeId, RaftNetwork, RaftStorage};

impl<'a, D: AppData, R: AppDataResponse, N: RaftNetwork<D>, S: RaftStorage<D, R>> NonVoterState<'a, D, R, N, S> {
    /// Handle the admin `init_with_config` command.
    #[tracing::instrument(level = "trace", skip(self))]
    pub(super) async fn handle_init_with_config(&mut self, mut members: HashSet<NodeId>) -> Result<(), InitializeError> {
        if self.core.last_log_index != 0 || self.core.current_term != 0 {
            tracing::error!({self.core.last_log_index, self.core.current_term}, "rejecting init_with_config request as last_log_index or current_term is 0");
            return Err(InitializeError::NotAllowed);
        }

        // Ensure given config contains this nodes ID as well.
        if !members.contains(&self.core.id) {
            members.insert(self.core.id);
        }

        // Build a new membership config from given init data & assign it as the new cluster
        // membership config in memory only.
        self.core.membership = MembershipConfig {
            members,
            members_after_consensus: None,
        };

        // Become a candidate and start campaigning for leadership. If this node is the only node
        // in the cluster, then become leader without holding an election. If members len == 1, we
        // know it is our ID due to the above code where we ensure our own ID is present.
        if self.core.membership.members.len() == 1 {
            self.core.current_term += 1;
            self.core.voted_for = Some(self.core.id);
            self.core.set_target_state(State::Leader);
            self.core.save_hard_state().await?;
        } else {
            self.core.set_target_state(State::Candidate);
        }

        Ok(())
    }
}

impl<'a, D: AppData, R: AppDataResponse, N: RaftNetwork<D>, S: RaftStorage<D, R>> LeaderState<'a, D, R, N, S> {
    /// Add a new node to the cluster as a non-voter, bringing it up-to-speed, and then responding
    /// on the given channel.
    #[tracing::instrument(level = "trace", skip(self, tx))]
    pub(super) fn add_member(&mut self, target: NodeId, tx: oneshot::Sender<Result<(), ChangeConfigError>>) {
        // Ensure the node doesn't already exist in the current config, in the set of new nodes
        // alreading being synced, or in the nodes being removed.
        if self.core.membership.members.contains(&target)
            || self
                .core
                .membership
                .members_after_consensus
                .as_ref()
                .map(|new| new.contains(&target))
                .unwrap_or(false)
            || self.non_voters.contains_key(&target)
        {
            tracing::debug!("target node is already a cluster member or is being synced");
            let _ = tx.send(Err(ChangeConfigError::Noop));
            return;
        }

        // Spawn a replication stream for the new member. Track state as a non-voter so that it
        // can be updated to be added to the cluster config once it has been brought up-to-date.
        let state = self.spawn_replication_stream(target);
        self.non_voters.insert(
            target,
            NonVoterReplicationState {
                state,
                is_ready_to_join: false,
                tx: Some(tx),
            },
        );
    }

    pub(super) fn remove_non_voter(&mut self, id: NodeId, tx: ChangeMembershipTx) {
        let want_to_remove_node_catching_up = match self.consensus_state {
            ConsensusState::CatchingUp { node, .. } => node == id,
            _ => false
        };
        if want_to_remove_node_catching_up {
            let _ = tx.send(Err(ChangeConfigError::ConfigChangeInProgress));
            return;
        }

        if let Some(state) = self.non_voters.remove(&id) {
            let _ = state.state.replstream.repltx.send(RaftEvent::Terminate);
            let _ = tx.send(Ok(()));
            return;
        }

        tracing::debug!("target node is is not recognized as a Non-Voter");
        let _ = tx.send(Err(ChangeConfigError::Noop));
    }

    #[tracing::instrument(level = "trace", skip(self, tx))]
    pub(super) async fn add_voter(&mut self, id: NodeId, tx: ChangeMembershipTx) {
        if self.core.membership.contains(&id) {
            let _ = tx.send(Err(ChangeConfigError::Noop));
            return;
        }

        let can_change_config = matches!(self.consensus_state, ConsensusState::Uniform);
        if !can_change_config {
            let _ = tx.send(Err(ChangeConfigError::ConfigChangeInProgress));
            return;
        }

        // If the new node already has a replication stream AND is caught up with its log, then
        // it is considered "ready to join", and we can immediately proceed with the config change.
        // Else, we first need to bring the node up-to-speed.
        let is_node_ready_to_join = match self.non_voters.get(&id) {
            Some(node) => node.is_ready_to_join,
            None => false
        };
        if !is_node_ready_to_join {
            // Spawn a replication stream for the new member. Track state as a non-voter so that it
            // can be updated to be added to the cluster config once it has been brought up-to-date.
            if !self.non_voters.contains_key(&id) {
                let state = self.spawn_replication_stream(id);
                self.non_voters.insert(
                    id,
                    NonVoterReplicationState {
                        state,
                        is_ready_to_join: false,
                        tx: None,
                    },
                );
            }

            // By entering CatchingUp state, we prevent other configuration
            // changes from happening. Bringing the new node up to speed
            // should happen pretty quickly, so this should not disrupt
            // other config changes for a long a time.
            //
            // Once the new node is brought up to speed,
            // add_voter will be called again automatically.
            self.consensus_state = ConsensusState::CatchingUp {
                node: id,
                tx,
            };

            return;
        }

        self.consensus_state = ConsensusState::ConfigChange {
            node: id,
            operation: ConfigChangeOperation::AddingNode,
        };

        // The new, changed configuration takes effect immediately.
        // Most importantly, we insert the new node into the self.nodes array,
        // which means that the new node counts towards the majority when replicating
        // the configuration change.
        let mut changed_members = self.core.membership.members.clone();
        changed_members.insert(id);
        self.core.membership.members = changed_members;

        // Here, it is safe to unwrap, as new nodes always start their lives as Non-Voters:
        //   * either, they are explicitly added as Non-Voters first,
        //   * or they're added as Non-Voters when calling add_voter.
        let non_voter_state = self.non_voters.remove(&id).unwrap();
        self.nodes.insert(id, non_voter_state.state);

        // Replicate the membership change using the normal Raft mechanism,
        // and respond on tx once done.
        self.propagate_membership_change(tx).await;
    }

    #[tracing::instrument(level = "trace", skip(self, tx))]
    async fn propagate_membership_change(&mut self, tx: ChangeMembershipTx) {
        tracing::debug!("members: {:?}", self.core.membership.members);
        tracing::debug!("non-voters: {:?}", self.non_voters.keys());
        tracing::debug!("nodes: {:?}", self.nodes.keys());

        // The config change is replicated using the normal
        // Raft mechanism.
        let payload = ClientWriteRequest::<D>::new_config(self.core.membership.clone());
        // Raft will notify us on this channel once the config change is committed,
        // and thus, replicated to the majority of the NEW configuration's nodes.
        let (tx_config_change_committed, rx_config_change_committed) = oneshot::channel();
        let entry = match self.append_payload_to_log(payload.entry).await {
            Ok(entry) => entry,
            Err(err) => {
                let _ = tx.send(Err(err.into()));
                return;
            }
        };
        let cr_entry = ClientRequestEntry::from_entry(entry, tx_config_change_committed);
        self.replicate_client_request(cr_entry).await;
        self.core.report_metrics();

        // Channel on which we wait for the config change to finish. Once the
        // value is sent over the channel, we can respond on our tx channel.
        let (tx_config_changed, rx_config_changed) = oneshot::channel();
        self.config_change_done_cb = Some(tx_config_changed);
        self.config_change_committed_cb.push(rx_config_change_committed);
        tokio::spawn(async move {
            let res = rx_config_changed
                .map_err(|_| RaftError::ShuttingDown)
                .into_future()
                .then(|res| {
                    futures::future::ready(match res {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(err)) => Err(ChangeConfigError::from(err)),
                        Err(err) => Err(ChangeConfigError::from(err)),
                    })
                })
                .await;
            let _ = tx.send(res);
        });
    }

    #[tracing::instrument(level = "trace", skip(self, tx))]
    pub(super) async fn remove_voter(&mut self, id: NodeId, tx: ChangeMembershipTx) {
        if !self.core.membership.contains(&id) {
            let _ = tx.send(Err(ChangeConfigError::Noop));
            return;
        }

        let can_change_config = matches!(self.consensus_state, ConsensusState::Uniform);
        if !can_change_config {
            let _ = tx.send(Err(ChangeConfigError::ConfigChangeInProgress));
            return;
        }

        self.consensus_state = ConsensusState::ConfigChange {
            node: id,
            operation: ConfigChangeOperation::RemovingNode,
        };

        // The new, changed configuration takes effect immediately.
        let mut changed_members = self.core.membership.members.clone();
        changed_members.remove(&id);
        self.core.membership.members = changed_members;

        if !self.core.membership.members.contains(&self.core.id) {
            self.is_stepping_down = true;
        }

        // Replicate the membership change using the normal Raft mechanism,
        // and respond on tx once done.
        self.propagate_membership_change(tx).await;
    }

    #[tracing::instrument(level = "trace", skip(self, tx))]
    pub(super) async fn change_membership(&mut self, members: HashSet<NodeId>, tx: ChangeMembershipTx) {
        // Ensure cluster will have at least one node.
        if members.is_empty() {
            let _ = tx.send(Err(ChangeConfigError::InoperableConfig));
            return;
        }

        // Only allow config updates when currently in a uniform consensus state.
        match &self.consensus_state {
            ConsensusState::Uniform => (),
            ConsensusState::NonVoterSync { .. } | ConsensusState::Joint { .. } | ConsensusState::ConfigChange { .. } | ConsensusState::CatchingUp { .. } => {
                let _ = tx.send(Err(ChangeConfigError::ConfigChangeInProgress));
                return;
            }
        }

        // Check the proposed config for any new nodes. If ALL new nodes already have replication
        // streams AND are ready to join, then we can immediately proceed with entering joint
        // consensus. Else, new nodes need to first be brought up-to-speed.
        //
        // Here, all we do is check to see which nodes still need to be synced, which determines
        // if we can proceed.
        let mut awaiting = HashSet::new();
        for new_node in members.difference(&self.core.membership.members) {
            match self.non_voters.get(&new_node) {
                // Node is ready to join.
                Some(node) if node.is_ready_to_join => continue,
                // Node has repl stream, but is not yet ready to join.
                Some(_) => (),
                // Node does not yet have a repl stream, spawn one.
                None => {
                    // Spawn a replication stream for the new member. Track state as a non-voter so that it
                    // can be updated to be added to the cluster config once it has been brought up-to-date.
                    let state = self.spawn_replication_stream(*new_node);
                    self.non_voters.insert(
                        *new_node,
                        NonVoterReplicationState {
                            state,
                            is_ready_to_join: false,
                            tx: None,
                        },
                    );
                }
            }
            awaiting.insert(*new_node);
        }
        // If there are new nodes which need to sync, then we need to wait until they are synced.
        // Once they've finished, this routine will be called again to progress further.
        if !awaiting.is_empty() {
            self.consensus_state = ConsensusState::NonVoterSync { awaiting, members, tx };
            return;
        }

        // Enter into joint consensus if we are not awaiting any new nodes.
        if !members.contains(&self.core.id) {
            self.is_stepping_down = true;
        }
        self.consensus_state = ConsensusState::Joint { is_committed: false };
        self.core.membership.members_after_consensus = Some(members);

        // Propagate the command as any other client request.
        let payload = ClientWriteRequest::<D>::new_config(self.core.membership.clone());
        let (tx_joint, rx_join) = oneshot::channel();
        let entry = match self.append_payload_to_log(payload.entry).await {
            Ok(entry) => entry,
            Err(err) => {
                let _ = tx.send(Err(err.into()));
                return;
            }
        };
        let cr_entry = ClientRequestEntry::from_entry(entry, tx_joint);
        self.replicate_client_request(cr_entry).await;
        self.core.report_metrics();

        // Setup channels for eventual response to the 2-phase config change.
        let (tx_cfg_change, rx_cfg_change) = oneshot::channel();
        self.propose_config_change_cb = Some(tx_cfg_change); // Once the entire process is done, this is our response channel.
        self.joint_consensus_cb.push(rx_join); // Receiver for when the joint consensus is committed.
        tokio::spawn(async move {
            let res = rx_cfg_change
                .map_err(|_| RaftError::ShuttingDown)
                .into_future()
                .then(|res| {
                    futures::future::ready(match res {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(err)) => Err(ChangeConfigError::from(err)),
                        Err(err) => Err(ChangeConfigError::from(err)),
                    })
                })
                .await;
            let _ = tx.send(res);
        });
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub(super) async fn handle_config_change_committed(&mut self, index: u64) -> Result<(), RaftError> {
        let node_to_remove = match self.consensus_state {
            ConsensusState::ConfigChange { node, operation }
                if operation == ConfigChangeOperation::RemovingNode => Some(node),
            _ => None,
        };

        if let Some(node) = node_to_remove {
            let mut should_immediately_remove_node = false;
            if let Some(replstate) = self.nodes.get_mut(&node) {
                if replstate.match_index >= index {
                    should_immediately_remove_node = true;
                } else {
                    replstate.remove_after_commit = Some(index);
                }
            }

            if should_immediately_remove_node {
                tracing::debug!({ target = node }, "removing target node from replication pool");
                if let Some(node) = self.nodes.remove(&node) {
                    let _ = node.replstream.repltx.send(RaftEvent::Terminate);
                }
            }
        }

        // Step down if needed.
        if self.is_stepping_down {
            tracing::debug!("raft node is stepping down");
            self.core.set_target_state(State::NonVoter);
            self.core.update_current_leader(UpdateCurrentLeader::Unknown);
        }

        self.consensus_state = ConsensusState::Uniform;
        self.core.report_metrics();

        Ok(())
    }

    /// Finalize the comitted joint consensus.
    #[tracing::instrument(level = "trace", skip(self))]
    pub(super) async fn finalize_joint_consensus(&mut self) -> Result<(), RaftError> {
        // Only proceed if it is safe to do so.
        if !self.consensus_state.is_joint_consensus_safe_to_finalize() {
            tracing::error!("attempted to finalize joint consensus when it was not safe to do so");
            return Ok(());
        }

        // Cut the cluster config over to the new membership config.
        if let Some(new_members) = self.core.membership.members_after_consensus.take() {
            self.core.membership.members = new_members;
        }
        self.consensus_state = ConsensusState::Uniform;

        // NOTE WELL: this implementation uses replication streams (src/replication/**) to replicate
        // entries. Nodes which do not exist in the new config will still have an active replication
        // stream until the current leader determines that they have replicated the config entry which
        // removes them from the cluster. At that point in time, the node will revert to non-voter state.
        //
        // HOWEVER, if an election takes place, the new leader will not have the old nodes in its config
        // and the old nodes may not revert to non-voter state using the above mechanism. That is fine.
        // The Raft spec accounts for this using the 3rd safety measure of cluster configuration changes
        // described at the very end of §6. This measure is already implemented and in place.

        // Propagate the next command as any other client request.
        let payload = ClientWriteRequest::<D>::new_config(self.core.membership.clone());
        let (tx_uniform, rx_uniform) = oneshot::channel();
        let entry = self.append_payload_to_log(payload.entry).await?;
        let cr_entry = ClientRequestEntry::from_entry(entry, tx_uniform);
        self.replicate_client_request(cr_entry).await;
        self.core.report_metrics();

        // Setup channel for eventual commitment of the uniform consensus config.
        self.uniform_consensus_cb.push(rx_uniform); // Receiver for when the uniform consensus is committed.
        Ok(())
    }

    /// Handle the commitment of a uniform consensus cluster configuration.
    #[tracing::instrument(level = "trace", skip(self))]
    pub(super) async fn handle_uniform_consensus_committed(&mut self, index: u64) -> Result<(), RaftError> {
        // Step down if needed.
        if self.is_stepping_down {
            tracing::debug!("raft node is stepping down");
            self.core.set_target_state(State::NonVoter);
            self.core.update_current_leader(UpdateCurrentLeader::Unknown);
            return Ok(());
        }

        // Remove any replication streams which have replicated this config & which are no longer
        // cluster members. All other replication streams which are no longer cluster members, but
        // which have not yet replicated this config will be marked for removal.
        let membership = &self.core.membership;
        let nodes_to_remove: Vec<_> = self
            .nodes
            .iter_mut()
            .filter(|(id, _)| !membership.contains(id))
            .filter_map(|(idx, replstate)| {
                if replstate.match_index >= index {
                    Some(*idx)
                } else {
                    replstate.remove_after_commit = Some(index);
                    None
                }
            })
            .collect();
        for node in nodes_to_remove {
            tracing::debug!({ target = node }, "removing target node from replication pool");
            if let Some(node) = self.nodes.remove(&node) {
                let _ = node.replstream.repltx.send(RaftEvent::Terminate);
            }
        }
        self.core.report_metrics();
        Ok(())
    }
}
