use serde::de::DeserializeOwned;
use serde::Serialize;

mod dequeue;
mod dequeue_at;
mod enqueue;
mod enqueue_at;
pub(crate) mod error;
mod helper;
pub(crate) mod lua;
pub(crate) mod queue;
mod redis_keys;

pub use {
    dequeue::{DequeueAction, DequeueSleep, DequeueStatus, FinishAction, SleepOnAction},
    dequeue_at::{DequeueAtAction, DequeueAtStatus},
    enqueue::{EnqueueAction, EnqueueStatus},
    enqueue_at::{EnqueueAtAction, EnqueueAtStatus},
    error::{YqError, YqResult, YqRunJobError},
    helper::decode_job,
    queue::Queue,
};

pub type JobType = std::borrow::Cow<'static, str>;

pub trait Job: Serialize + DeserializeOwned {
    const JOB_TYPE: JobType;

    type State: Clone + 'static;

    const LOCK_MS: isize = -1;
}

pub(crate) type ArcString = std::sync::Arc<String>;
