use crate::{redis_keys, ArcString};
use std::sync::Arc;

const DEFAULT_PREFIX: &str = "yq";
const DEFAULT_QUEUE: &str = "0";
const DEFAULT_LOCK_MS: i64 = 60 * 60; // 60 minutes

#[derive(Clone)]
pub struct Queue {
    pub queue_name: ArcString,
    pub(crate) default_lock_ms: i64,
    pub(crate) mid_seq_key: ArcString,
    pub(crate) messages_key: ArcString,
    pub(crate) lock_times_key: ArcString,
    pub(crate) locks_key: ArcString,
    pub(crate) done_key: ArcString,
    pub err_messages_key: ArcString,
    pub err_key: ArcString,
    pub(crate) mids_ready_key: ArcString,
    pub(crate) mid_circle_key: ArcString,
    pub(crate) ndry_runs_key: ArcString,
    pub(crate) isleep_a_key: ArcString,
    pub(crate) isleep_b_key: ArcString,
    pub(crate) schedule_key: ArcString,
}

impl Default for Queue {
    fn default() -> Self {
        Queue::new(
            Arc::new(DEFAULT_PREFIX.into()),
            Arc::new(DEFAULT_QUEUE.into()),
        )
    }
}

impl Queue {
    pub(crate) fn new(prefix: ArcString, queue_name: ArcString) -> Self {
        let mid_seq_key = redis_keys::mid_seq_key(&prefix, &queue_name);
        let messages_key = redis_keys::messages_key(&prefix, &queue_name);
        let lock_times_key = redis_keys::lock_times_key(&prefix, &queue_name);
        let locks_key = redis_keys::locks_key(&prefix, &queue_name);
        let err_messages_key = redis_keys::err_messages_key(&prefix, &queue_name);
        let err_key = redis_keys::err_key(&prefix, &queue_name);
        let done_key = redis_keys::done_key(&prefix, &queue_name);
        let mids_ready_key = redis_keys::mids_ready_key(&prefix, &queue_name);
        let mid_circle_key = redis_keys::mid_circle_key(&prefix, &queue_name);
        let ndry_runs_key = redis_keys::ndry_runs_key(&prefix, &queue_name);
        let isleep_a_key = redis_keys::isleep_a_key(&prefix, &queue_name);
        let isleep_b_key = redis_keys::isleep_b_key(&prefix, &queue_name);
        let schedule_key = redis_keys::schedule_key(&prefix);

        Self {
            queue_name,
            default_lock_ms: DEFAULT_LOCK_MS,
            mid_seq_key,
            messages_key,
            lock_times_key,
            locks_key,
            done_key,
            err_messages_key,
            err_key,
            mids_ready_key,
            mid_circle_key,
            ndry_runs_key,
            isleep_a_key,
            isleep_b_key,
            schedule_key,
        }
    }
}
