use serde::de::DeserializeOwned;
use serde::Serialize;

pub mod asynk;
pub mod error;
mod helper;
mod lua;
pub(crate) mod queue;
mod redis_keys;
pub mod sync;

pub type JobType = std::borrow::Cow<'static, str>;

pub(crate) type ArcString = std::sync::Arc<String>;

pub trait Job: Serialize + DeserializeOwned {
    const JOB_TYPE: JobType;

    type State: Clone + 'static;

    const LOCK_MS: isize = -1;
}
