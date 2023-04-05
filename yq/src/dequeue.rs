use crate::helper::{read_redis_value_as_int, read_redis_value_as_str};
use crate::Queue;
use redis::{FromRedisValue, RedisResult, Script, ScriptInvocation};

#[derive(Clone)]
pub struct DequeueAction {
    script: Script,
    queue: Queue,
}

impl DequeueAction {
    pub fn new(queue: Queue) -> Self {
        Self {
            script: Script::new(crate::lua::DEQUEUE),
            queue,
        }
    }

    pub fn prepare_invoke(&self, now: i64) -> ScriptInvocation {
        let mut invoke = self.script.prepare_invoke();
        invoke
            .key(self.queue.messages_key.as_str())
            .key(self.queue.lock_times_key.as_str())
            .key(self.queue.locks_key.as_str())
            .key(self.queue.done_key.as_str())
            .key(self.queue.mids_ready_key.as_str())
            .key(self.queue.mid_circle_key.as_str())
            .key(self.queue.ndry_runs_key.as_str())
            .key(self.queue.isleep_b_key.as_str());

        invoke.arg(now * 1000).arg(self.queue.default_lock_ms);

        invoke
    }
}

#[derive(Clone)]
pub struct FinishAction {
    queue: Queue,
}

impl FinishAction {
    pub fn new(queue: Queue) -> Self {
        Self { queue }
    }

    pub fn prepare_invoke(&self, job_id: i64) -> redis::Cmd {
        redis::Cmd::sadd(self.queue.done_key.as_str(), job_id)
    }
}

#[derive(Clone)]
pub struct SleepOnAction {
    queue: Queue,
}

impl SleepOnAction {
    pub fn new(queue: Queue) -> Self {
        Self { queue }
    }

    pub fn prepare_invoke(&self, dequeue_sleep: DequeueSleep) -> redis::Cmd {
        let sleep_time = if dequeue_sleep.ndry_runs > 6 {
            18
        } else if dequeue_sleep.ndry_runs <= 0 {
            1
        } else {
            //                        dequeue_sleep.ndry_runs
            dequeue_sleep.ndry_runs * 3
        };

        let (src_key, dst_key) = match dequeue_sleep.sleep_on {
            SleepOn::SleepOnA => (
                self.queue.isleep_a_key.as_str(),
                self.queue.isleep_b_key.as_str(),
            ),
            SleepOn::SleepOnB => (
                self.queue.isleep_b_key.as_str(),
                self.queue.isleep_a_key.as_str(),
            ),
        };

        tracing::trace!("sleep_on - {} secs", sleep_time);

        redis::Cmd::brpoplpush(src_key, dst_key, sleep_time as usize)
    }
}

#[derive(Debug)]
pub enum DequeueStatus {
    Sleep(DequeueSleep),
    Handle(DequeueHandle),
    Skip(DequeueSkip),
    Unknown(String),
}

#[derive(Debug)]
pub struct DequeueSleep {
    _reason: String,
    pub(crate) sleep_on: SleepOn,
    pub(crate) ndry_runs: i64,
}

impl Default for DequeueSleep {
    fn default() -> Self {
        DequeueSleep {
            _reason: "".to_string(),
            sleep_on: SleepOn::SleepOnA,
            ndry_runs: 1,
        }
    }
}

impl DequeueSleep {
    fn try_new<'a>(mut iter: impl Iterator<Item = &'a redis::Value>) -> RedisResult<Self> {
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
pub struct DequeueHandle {
    pub mid: i64,
    pub mcontent: String,
    _lock_ms: i64,
}

impl DequeueHandle {
    fn try_new<'a>(mut iter: impl Iterator<Item = &'a redis::Value>) -> RedisResult<Self> {
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
pub struct DequeueSkip {
    _reason: String,
    _mid: String,
}

impl DequeueSkip {
    fn try_new<'a>(mut iter: impl Iterator<Item = &'a redis::Value>) -> RedisResult<Self> {
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

impl TryFrom<&[redis::Value]> for DequeueStatus {
    type Error = redis::RedisError;

    fn try_from(values: &[redis::Value]) -> Result<Self, Self::Error> {
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
    fn from_redis_value(v: &redis::Value) -> RedisResult<Self> {
        match v {
            redis::Value::Bulk(bulk) => DequeueStatus::try_from(bulk.as_slice()),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "invalid dequeue status - invalid value type",
                format!("{v:?}"),
            ))),
        }
    }
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
