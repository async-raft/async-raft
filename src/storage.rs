//! The RaftStorage interface and message types.

use std::sync::Arc;

use actix::{
    dev::ToEnvelope,
    prelude::*,
};
use futures::sync::{mpsc::UnboundedReceiver, oneshot::Sender};
use serde::{Serialize, Deserialize};

use crate::{
    AppData, AppDataResponse, AppError, NodeId,
    messages,
};

//////////////////////////////////////////////////////////////////////////////
// GetInitialState ///////////////////////////////////////////////////////////

/// A request from Raft to get Raft's state information from storage.
///
/// When the Raft actor is first started, it will call this interface on the storage system to
/// fetch the last known state from stable storage. If no such entry exists due to being the
/// first time the node has come online, then the default value for `InitialState` should be used.
///
/// ### pro tip
/// The storage impl may need to look in a few different places to accurately respond to this
/// request. That last entry in the log for `last_log_index` & `last_log_term`; the node's hard
/// state record; and the index of the last log applied to the state machine.
pub struct GetInitialState<E: AppError> {
    marker: std::marker::PhantomData<E>,
}

impl<E: AppError> GetInitialState<E> {
    // Create a new instance.
    pub fn new() -> Self {
        Self{marker: std::marker::PhantomData}
    }
}

impl<E: AppError> Message for GetInitialState<E> {
    type Result = Result<InitialState, E>;
}

/// A struct used to represent the initial state which a Raft node needs when first starting.
#[derive(Clone, Debug, Default)]
pub struct InitialState {
    /// The index of the last entry.
    pub last_log_index: u64,
    /// The term of the last log entry.
    pub last_log_term: u64,
    /// The index of the last log applied to the state machine.
    pub last_applied_log: u64,
    /// The saved hard state of the node.
    pub hard_state: HardState,
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// GetLogEntries /////////////////////////////////////////////////////////////////////////////////

/// A request from Raft to get a series of log entries from storage.
///
/// The start value is inclusive in the search and the stop value is non-inclusive:
/// `[start, stop)`.
pub struct GetLogEntries<D: AppData, E: AppError> {
    pub start: u64,
    pub stop: u64,
    marker_data: std::marker::PhantomData<D>,
    marker_error: std::marker::PhantomData<E>,
}

impl<D: AppData, E: AppError> GetLogEntries<D, E> {
    // Create a new instance.
    pub fn new(start: u64, stop: u64) -> Self {
        Self{start, stop, marker_data: std::marker::PhantomData, marker_error: std::marker::PhantomData}
    }
}

impl<D: AppData, E: AppError> Message for GetLogEntries<D, E> {
    type Result = Result<Vec<messages::Entry<D>>, E>;
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// AppendEntryToLog //////////////////////////////////////////////////////////////////////////////

/// A request from Raft to append a new entry to the log.
///
/// These requests come about via client requests, and as such, this is the only RaftStorage
/// interface which is allowed to return errors which will not cause Raft to shutdown. Application
/// errors coming from this interface will be sent back as-is to the call point where your
/// application originally presented the client request to Raft.
///
/// This property of error handling allows you to keep your application logic as close to the
/// storage layer as needed.
pub struct AppendEntryToLog<D: AppData, E: AppError> {
    pub entry: Arc<messages::Entry<D>>,
    marker: std::marker::PhantomData<E>,
}

impl<D: AppData, E: AppError> AppendEntryToLog<D, E> {
    // Create a new instance.
    pub fn new(entry: Arc<messages::Entry<D>>) -> Self {
        Self{entry, marker: std::marker::PhantomData}
    }
}

impl<D: AppData, E: AppError> Message for AppendEntryToLog<D, E> {
    type Result = Result<(), E>;
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// ReplicateToLog ////////////////////////////////////////////////////////////////////////////////

/// A request from Raft to replicate a payload of entries to the log.
///
/// These requests come about via the Raft leader's replication process. An error coming from this
/// interface will cause Raft to shutdown, as this is not where application logic should be
/// returning application specific errors. Application specific constraints may only be enforced
/// in the `AppendEntryToLog` handler.
///
/// Though the entries will always be presented in order, each entry's index should be used to
/// determine its location to be written in the log, as logs may need to be overwritten under
/// some circumstances.
pub struct ReplicateToLog<D: AppData, E: AppError> {
    pub entries: Arc<Vec<messages::Entry<D>>>,
    marker: std::marker::PhantomData<E>,
}

impl<D: AppData, E: AppError> ReplicateToLog<D, E> {
    // Create a new instance.
    pub fn new(entries: Arc<Vec<messages::Entry<D>>>) -> Self {
        Self{entries, marker: std::marker::PhantomData}
    }
}

impl<D: AppData, E: AppError> Message for ReplicateToLog<D, E> {
    type Result = Result<(), E>;
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// ApplyEntryToStateMachine //////////////////////////////////////////////////////////////////////

/// A request from Raft to apply the given log entry to the state machine.
///
/// This handler is called as part of the client request path. Client requests which are
/// configured to respond after they have been `Applied` will wait until after this handler
/// returns before issuing a response to the client request.
///
/// The Raft protocol guarantees that only logs which have been _committed_, that is, logs which
/// have been replicated to a majority of the cluster, will be applied to the state machine.
pub struct ApplyEntryToStateMachine<D: AppData, R: AppDataResponse, E: AppError> {
    pub payload: Arc<messages::Entry<D>>,
    marker0: std::marker::PhantomData<R>,
    marker1: std::marker::PhantomData<E>,
}

impl<D: AppData, R: AppDataResponse, E: AppError> ApplyEntryToStateMachine<D, R, E> {
    // Create a new instance.
    pub fn new(payload: Arc<messages::Entry<D>>) -> Self {
        Self{payload, marker0: std::marker::PhantomData, marker1: std::marker::PhantomData}
    }
}

impl<D: AppData, R: AppDataResponse, E: AppError> Message for ApplyEntryToStateMachine<D, R, E> {
    type Result = Result<R, E>;
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// ReplicateToStateMachine ///////////////////////////////////////////////////////////////////////

/// A request from Raft to apply the given payload of entries to the state machine, as part of replication.
///
/// The Raft protocol guarantees that only logs which have been _committed_, that is, logs which
/// have been replicated to a majority of the cluster, will be applied to the state machine.
pub struct ReplicateToStateMachine<D: AppData, E: AppError> {
    pub payload: Vec<messages::Entry<D>>,
    marker: std::marker::PhantomData<E>,
}

impl<D: AppData, E: AppError> ReplicateToStateMachine<D, E> {
    // Create a new instance.
    pub fn new(payload: Vec<messages::Entry<D>>) -> Self {
        Self{payload, marker: std::marker::PhantomData}
    }
}

impl<D: AppData, E: AppError> Message for ReplicateToStateMachine<D, E> {
    type Result = Result<(), E>;
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// CreateSnapshot ////////////////////////////////////////////////////////////////////////////////

/// A request from Raft to have a new snapshot created which covers the current breadth
/// of the log.
///
/// See the [storage chapter of the guide](https://railgun-rs.github.io/actix-raft/storage.html#CreateSnapshot)
/// for details on how to implement this handler.
pub struct CreateSnapshot<E: AppError> {
    /// The new snapshot should start from entry `0` and should cover all entries through the
    /// index specified here, inclusive.
    pub through: u64,
    marker: std::marker::PhantomData<E>,
}

impl<E: AppError> CreateSnapshot<E> {
    // Create a new instance.
    pub fn new(through: u64) -> Self {
        Self{through, marker: std::marker::PhantomData}
    }
}

impl<E: AppError> Message for CreateSnapshot<E> {
    type Result = Result<CurrentSnapshotData, E>;
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// InstallSnapshot ///////////////////////////////////////////////////////////////////////////////

/// A request from Raft to have a new snapshot written to disk and installed.
///
/// See the [storage chapter of the guide](https://railgun-rs.github.io/actix-raft/storage.html#InstallSnapshot)
/// for details on how to implement this handler.
pub struct InstallSnapshot<E: AppError> {
    /// The term which the final entry of this snapshot covers.
    pub term: u64,
    /// The index of the final entry which this snapshot covers.
    pub index: u64,
    /// A stream of data chunks for this snapshot.
    pub stream: UnboundedReceiver<InstallSnapshotChunk>,
    marker: std::marker::PhantomData<E>,
}

impl<E: AppError> InstallSnapshot<E> {
    // Create a new instance.
    pub fn new(term: u64, index: u64, stream: UnboundedReceiver<InstallSnapshotChunk>) -> Self {
        Self{term, index, stream, marker: std::marker::PhantomData}
    }
}

impl<E: AppError> Message for InstallSnapshot<E> {
    type Result = Result<(), E>;
}

/// A chunk of snapshot data.
pub struct InstallSnapshotChunk {
    /// The byte offset where chunk is positioned in the snapshot file.
    pub offset: u64,
    /// The raw bytes of the snapshot chunk, starting at `offset`.
    pub data: Vec<u8>,
    /// Will be `true` if this is the last chunk in the snapshot.
    pub done: bool,
    /// A callback channel to indicate when the chunk has been successfully written.
    pub cb: Sender<()>,
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// GetCurrentSnapshot ////////////////////////////////////////////////////////////////////////////

/// A request from Raft to get metadata of the current snapshot.
///
/// ### implementation algorithm
/// Implementation for this type's handler should be quite simple. Check the configured snapshot
/// directory for any snapshot files. A proper implementation will only ever have one
/// active snapshot, though another may exist while it is being created. As such, it is
/// recommended to use a file naming pattern which will allow for easily distinguishing between
/// the current live snapshot, and any new snapshot which is being created.
pub struct GetCurrentSnapshot<E: AppError> {
    marker: std::marker::PhantomData<E>,
}

impl<E: AppError> GetCurrentSnapshot<E> {
    // Create a new instance.
    pub fn new() -> Self {
        Self{marker: std::marker::PhantomData}
    }
}

impl<E: AppError> Message for GetCurrentSnapshot<E> {
    type Result = Result<Option<CurrentSnapshotData>, E>;
}

/// The data associated with the current snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct CurrentSnapshotData {
    /// The snapshot entry's term.
    pub term: u64,
    /// The snapshot entry's index.
    pub index: u64,
    /// The latest membership configuration covered by the snapshot.
    pub membership: messages::MembershipConfig,
    /// The snapshot entry's pointer to the snapshot file.
    pub pointer: messages::EntrySnapshotPointer,
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// SaveHardState /////////////////////////////////////////////////////////////////////////////////

/// A request from Raft to save its HardState.
pub struct SaveHardState<E: AppError>{
    pub hs: HardState,
    marker: std::marker::PhantomData<E>,
}

impl<E: AppError> SaveHardState<E> {
    // Create a new instance.
    pub fn new(hs: HardState) -> Self {
        Self{hs, marker: std::marker::PhantomData}
    }
}

impl<E: AppError> Message for SaveHardState<E> {
    type Result = Result<(), E>;
}

/// A record holding the hard state of a Raft node.
///
/// This model derives serde's traits for easily (de)serializing this
/// model for storage & retrieval.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct HardState {
    /// The last recorded term observed by this system.
    pub current_term: u64,
    /// The ID of the node voted for in the `current_term`.
    pub voted_for: Option<NodeId>,
    /// The cluster membership configuration.
    pub membership: messages::MembershipConfig,
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// RaftStorage ///////////////////////////////////////////////////////////////////////////////////

/// A trait defining the interface of a Raft storage actor.
///
/// See the [storage chapter of the guide](https://railgun-rs.github.io/actix-raft/storage.html#InstallSnapshot)
/// for details and discussion on this trait and how to implement it.
pub trait RaftStorage<D, R, E>: 'static
    where
        D: AppData,
        R: AppDataResponse,
        E: AppError,
{
    /// The type to use as the storage actor. Should just be Self.
    type Actor: Actor<Context=Self::Context> +
        Handler<GetInitialState<E>> +
        Handler<SaveHardState<E>> +
        Handler<GetLogEntries<D, E>> +
        Handler<AppendEntryToLog<D, E>> +
        Handler<ReplicateToLog<D, E>> +
        Handler<ApplyEntryToStateMachine<D, R, E>> +
        Handler<ReplicateToStateMachine<D, E>> +
        Handler<CreateSnapshot<E>> +
        Handler<InstallSnapshot<E>> +
        Handler<GetCurrentSnapshot<E>>;

    /// The type to use as the storage actor's context. Should be `Context<Self>` or `SyncContext<Self>`.
    type Context: ActorContext +
        ToEnvelope<Self::Actor, GetInitialState<E>> +
        ToEnvelope<Self::Actor, SaveHardState<E>> +
        ToEnvelope<Self::Actor, GetLogEntries<D, E>> +
        ToEnvelope<Self::Actor, AppendEntryToLog<D, E>> +
        ToEnvelope<Self::Actor, ReplicateToLog<D, E>> +
        ToEnvelope<Self::Actor, ApplyEntryToStateMachine<D, R, E>> +
        ToEnvelope<Self::Actor, ReplicateToStateMachine<D, E>> +
        ToEnvelope<Self::Actor, CreateSnapshot<E>> +
        ToEnvelope<Self::Actor, InstallSnapshot<E>> +
        ToEnvelope<Self::Actor, GetCurrentSnapshot<E>>;
}
