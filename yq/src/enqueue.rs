use crate::helper::{encode_job, read_redis_value_as_int, read_redis_value_as_str};
use crate::{Job, Queue, YqResult};
use redis::{FromRedisValue, RedisResult, Script, ScriptInvocation};

pub struct EnqueueAction {
    script: Script,
    queue: Queue,
}

impl EnqueueAction {
    pub fn new(queue: Queue) -> Self {
        Self {
            script: Script::new(crate::lua::ENQUEUE),
            queue,
        }
    }

    pub fn prepare_invoke<J: Job>(&self, job: &J) -> YqResult<ScriptInvocation> {
        let mut invoke = self.script.prepare_invoke();
        invoke
            .key(self.queue.mid_seq_key.as_str())
            .key(self.queue.messages_key.as_str())
            .key(self.queue.lock_times_key.as_str())
            .key(self.queue.mids_ready_key.as_str())
            .key(self.queue.mid_circle_key.as_str())
            .key(self.queue.isleep_a_key.as_str())
            .key(self.queue.isleep_b_key.as_str());

        let job_data = encode_job(job)?;
        invoke.arg(&job_data).arg(J::LOCK_MS);

        Ok(invoke)
    }
}

#[derive(Debug)]
pub enum EnqueueStatus {
    Added(EnqueueStatusAdded),
    Unknown(String),
}

#[derive(Debug)]
pub struct EnqueueStatusAdded {
    pub mid: i64,
}

impl TryFrom<&[redis::Value]> for EnqueueStatus {
    type Error = redis::RedisError;

    fn try_from(values: &[redis::Value]) -> Result<Self, Self::Error> {
        let mut iter = values.iter();
        let action =
            read_redis_value_as_str(iter.next(), "invalid enqueue status - invalid action")?;

        let status = match action.as_ref() {
            "added" => {
                let _sleep_on = read_redis_value_as_str(
                    iter.next(),
                    "invalid enqueue status - invalid sleep_on",
                )?;
                let mid =
                    read_redis_value_as_int(iter.next(), "invalid enqueue status - invalid mid")?;
                EnqueueStatus::Added(EnqueueStatusAdded { mid })
            }
            _ => EnqueueStatus::Unknown(format!("{values:?}")),
        };

        Ok(status)
    }
}

impl FromRedisValue for EnqueueStatus {
    fn from_redis_value(v: &redis::Value) -> RedisResult<Self> {
        match v {
            redis::Value::Bulk(bulk) => EnqueueStatus::try_from(bulk.as_slice()),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "invalid dequeue status - invalid value type",
                format!("{v:?}"),
            ))),
        }
    }
}
