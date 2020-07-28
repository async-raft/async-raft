//! Raft metrics for observability.
//!
//! Applications may use this data in whatever way is needed. The obvious use cases are to expose
//! these metrics to a metrics collection system like Prometheus. Applications may also
//! use this data to trigger events within higher levels of the parent application.
//!
//! Metrics are observed on a running Raft node via the `Raft::metrics()` method, which will
//! return a stream of metrics.

use crate::NodeId;
use crate::raft::MembershipConfig;

/// All possible states of a Raft node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// The node is completely passive; replicating entries, but neither voting nor timing out.
    NonVoter,
    /// The node is replicating logs from the leader.
    Follower,
    /// The node is campaigning to become the cluster leader.
    Candidate,
    /// The node is the Raft cluster leader.
    Leader,
}

/// Baseline metrics of the current state of the subject Raft node.
///
/// See the [module level documentation](TODO:)
/// for more details.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftMetrics {
    /// The ID of the Raft node.
    pub id: NodeId,
    /// The state of the Raft node.
    pub state: State,
    /// The current term of the Raft node.
    pub current_term: u64,
    /// The last log index to be appended to this Raft node's log.
    pub last_log_index: u64,
    /// The last log index to be applied to this Raft node's state machine.
    pub last_applied: u64,
    /// The current cluster leader.
    pub current_leader: Option<NodeId>,
    /// The current membership config of the cluster.
    pub membership_config: MembershipConfig,
}

impl RaftMetrics {
    pub(crate) fn new_initial(id: NodeId) -> Self {
        let membership_config = MembershipConfig::new_initial(id);
        Self{id, state: State::Follower, current_term: 0, last_log_index: 0, last_applied: 0, current_leader: None, membership_config}
    }
}