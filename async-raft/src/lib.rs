#![cfg_attr(feature = "docinclude", feature(external_doc))]
#![cfg_attr(feature = "docinclude", doc(include = "../README.md"))]

pub mod config;
mod core;
pub mod error;
pub mod metrics;
pub mod network;
pub mod raft;
mod replication;
pub mod storage;

use std::error::Error;
use std::fmt::Debug;

use serde::{de::DeserializeOwned, Serialize};

pub use crate::{
    config::{Config, ConfigBuilder, SnapshotPolicy},
    core::State,
    error::{ChangeConfigError, ClientWriteError, ConfigError, InitializeError, RaftError},
    metrics::RaftMetrics,
    network::RaftNetwork,
    raft::Raft,
    storage::RaftStorage,
};
pub use async_trait;

/// A Raft node's ID.
pub type NodeId = u64;

/// A trait defining application specific data.
///
/// The intention of this trait is that applications which are using this crate will be able to
/// use their own concrete data types throughout their application without having to serialize and
/// deserialize their data as it goes through Raft. Instead, applications can present their data
/// models as-is to Raft, Raft will present it to the application's `RaftStorage` impl when ready,
/// and the application may then deal with the data directly in the storage engine without having
/// to do a preliminary deserialization.
pub trait AppData: Clone + Debug + Send + Sync + Serialize + DeserializeOwned + 'static {}

/// A trait defining application specific response data.
///
/// The intention of this trait is that applications which are using this crate will be able to
/// use their own concrete data types for returning response data from the storage layer when an
/// entry is applied to the state machine as part of a client request (this is not used during
/// replication). This allows applications to seamlessly return application specific data from
/// their storage layer, up through Raft, and back into their application for returning
/// data to clients.
///
/// This type is specifically intended to represent the successful application of a client request
/// to the storage engine. For errors, use the `AppError` type.
pub trait AppDataResponse: Clone + Debug + Send + Sync + Serialize + DeserializeOwned + 'static {}

/// A trait defining an application specific error type.
///
/// The intention of this trait is that applications which are using this crate will be able to
/// use their own concrete error types for returned error variants from the storage layer. Under
/// normal circumstances, if Raft detects that an error has been returned from the storage layer,
/// it will go into Shutdown. In a few specific cases, namely in `apply_entry_to_state_machine`,
/// applications are allowed to return an instance of this type as an error, and in such cases the
/// error will be propagated without causing shutdown.
///
/// This allows for an ergonomic error control flow in `RaftStorage` implementations, but also
/// gives this crate a mechanism for being able to differentiate between fatal vs application
/// errors in the storage system.
pub trait AppError: Clone + Debug + Error + Send + Sync + 'static {}
