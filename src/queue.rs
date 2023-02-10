use crate::{redis_keys, ArcString};
use redis::{FromRedisValue, RedisResult, Value};
use std::borrow::Cow;
use std::sync::Arc;

const DEFAULT_PREFIX: &str = "yq";
const DEFAULT_QUEUE: &str = "0";
const DEFAULT_LOCK_MS: i64 = 60 * 60; // 60 minutes

#[derive(Clone)]
pub(crate) struct Queue {
    pub(crate) queue_name: ArcString,
    pub(crate) default_lock_ms: i64,
    pub(crate) mid_seq_key: ArcString,
    pub(crate) messages_key: ArcString,
    pub(crate) lock_times_key: ArcString,
    pub(crate) locks_key: ArcString,
    pub(crate) done_key: ArcString,
    pub(crate) err_messages_key: ArcString,
    pub(crate) err_key: ArcString,
    pub(crate) mids_ready_key: ArcString,
    pub(crate) mid_circle_key: ArcString,
    pub(crate) ndry_runs_key: ArcString,
    pub(crate) isleep_a_key: ArcString,
    pub(crate) isleep_b_key: ArcString,
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
        }
    }
}

#[derive(Debug)]
pub(crate) enum EnqueueStatus {
    Added(EnqueueStatusAdded),
    Unknown(String),
}

#[derive(Debug)]
pub(crate) struct EnqueueStatusAdded {
    _sleep_on: SleepOn,
    pub(crate) mid: i64,
}

impl TryFrom<&[Value]> for EnqueueStatus {
    type Error = redis::RedisError;

    fn try_from(values: &[Value]) -> Result<Self, Self::Error> {
        let mut iter = values.iter();
        let action =
            read_redis_value_as_str(iter.next(), "invalid enqueue status - invalid action")?;

        let status = match action.as_ref() {
            "added" => {
                let sleep_on = read_redis_value_as_str(
                    iter.next(),
                    "invalid enqueue status - invalid sleep_on",
                )?;
                let sleep_on = SleepOn::from(sleep_on.as_ref());
                let mid =
                    read_redis_value_as_int(iter.next(), "invalid enqueue status - invalid mid")?;
                EnqueueStatus::Added(EnqueueStatusAdded {
                    _sleep_on: sleep_on,
                    mid,
                })
            }
            _ => EnqueueStatus::Unknown(format!("{values:?}")),
        };

        Ok(status)
    }
}

impl FromRedisValue for EnqueueStatus {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match v {
            Value::Bulk(bulk) => EnqueueStatus::try_from(bulk.as_slice()),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "invalid dequeue status - invalid value type",
                format!("{v:?}"),
            ))),
        }
    }
}

#[derive(Debug)]
pub(crate) enum DequeueStatus {
    Sleep(DequeueSleep),
    Handle(DequeueHandle),
    Skip(DequeueSkip),
    Unknown(String),
}

#[derive(Debug)]
pub(crate) struct DequeueSleep {
    _reason: String,
    pub(crate) sleep_on: SleepOn,
    pub(crate) ndry_runs: i64,
}

#[derive(Debug)]
pub(crate) enum SleepOn {
    SleepOnA,
    SleepOnB,
}

impl From<&str> for SleepOn {
    fn from(value: &str) -> Self {
        match value {
            "a" => SleepOn::SleepOnA,
            _ => SleepOn::SleepOnB,
        }
    }
}

impl DequeueSleep {
    fn try_new<'a>(mut iter: impl Iterator<Item = &'a Value>) -> RedisResult<Self> {
        let reason = read_redis_value_as_str(
            iter.next(),
            "invalid dequeue status - sleep - invalid reason",
        )?;
        let sleep_on = read_redis_value_as_str(
            iter.next(),
            "invalid dequeue status - sleep - invalid sleep_on",
        )?;
        let ndry_runs = read_redis_value_as_int(
            iter.next(),
            "invalid dequeue status - sleep - invalid ndry_runs",
        )?;
        Ok(DequeueSleep {
            _reason: reason.into_owned(),
            sleep_on: SleepOn::from(sleep_on.as_ref()),
            ndry_runs,
        })
    }
}

#[derive(Debug)]
pub(crate) struct DequeueHandle {
    pub(crate) mid: i64,
    pub(crate) mcontent: String,
    _lock_ms: i64,
}

impl DequeueHandle {
    fn try_new<'a>(mut iter: impl Iterator<Item = &'a Value>) -> RedisResult<Self> {
        let mid =
            read_redis_value_as_str(iter.next(), "invalid dequeue status - handle - invalid mid")?;
        let mid = mid.parse::<i64>().map_err(|err| {
            redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "invalid dequeue status - handle - invalid mid",
                err.to_string(),
            ))
        })?;
        let mcontent = read_redis_value_as_str(
            iter.next(),
            "invalid dequeue status - handle - invalid mcontent",
        )?;
        let lock_ms = read_redis_value_as_int(
            iter.next(),
            "invalid dequeue status - handle - invalid lock_ms",
        )?;

        Ok(DequeueHandle {
            mid,
            mcontent: mcontent.into_owned(),
            _lock_ms: lock_ms,
        })
    }
}

#[derive(Debug)]
pub(crate) struct DequeueSkip {
    _reason: String,
    _mid: String,
}

impl DequeueSkip {
    fn try_new<'a>(mut iter: impl Iterator<Item = &'a Value>) -> RedisResult<Self> {
        let reason = read_redis_value_as_str(
            iter.next(),
            "invalid dequeue status - skip - invalid reason",
        )?;

        let mid =
            read_redis_value_as_str(iter.next(), "invalid dequeue status - skip - invalid mid")?;

        Ok(DequeueSkip {
            _reason: reason.into_owned(),
            _mid: mid.into_owned(),
        })
    }
}

impl TryFrom<&[Value]> for DequeueStatus {
    type Error = redis::RedisError;

    fn try_from(values: &[Value]) -> Result<Self, Self::Error> {
        let mut iter = values.iter();
        let action =
            read_redis_value_as_str(iter.next(), "invalid dequeue status - invalid action")?;

        let status = match action.as_ref() {
            "sleep" => DequeueStatus::Sleep(DequeueSleep::try_new(iter)?),
            "handle" => DequeueStatus::Handle(DequeueHandle::try_new(iter)?),
            "skip" => DequeueStatus::Skip(DequeueSkip::try_new(iter)?),
            _ => DequeueStatus::Unknown(format!("{values:?}")),
        };

        Ok(status)
    }
}

impl FromRedisValue for DequeueStatus {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match v {
            Value::Bulk(bulk) => DequeueStatus::try_from(bulk.as_slice()),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "invalid dequeue status - invalid value type",
                format!("{v:?}"),
            ))),
        }
    }
}

fn read_redis_value_as_str<'a>(
    v: Option<&'a Value>,
    err_desc: &'static str,
) -> RedisResult<Cow<'a, str>> {
    let v = match v {
        Some(v) => v,
        None => {
            return Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                err_desc,
            )))
        }
    };

    match v {
        Value::Data(d) => Ok(String::from_utf8_lossy(d.as_slice())),
        _ => Err(redis::RedisError::from((
            redis::ErrorKind::ResponseError,
            err_desc,
            format!("{v:?}"),
        ))),
    }
}

fn read_redis_value_as_int(v: Option<&Value>, err_desc: &'static str) -> RedisResult<i64> {
    let v = match v {
        Some(v) => v,
        None => {
            return Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                err_desc,
            )))
        }
    };

    match v {
        Value::Int(i) => Ok(*i),
        _ => Err(redis::RedisError::from((
            redis::ErrorKind::ResponseError,
            err_desc,
            format!("{v:?}"),
        ))),
    }
}
