use crate::helper::{encode_job, read_redis_value_as_int, read_redis_value_as_str};
use crate::{Job, Queue, YqResult};
use redis::{FromRedisValue, RedisResult, Script, ScriptInvocation};

#[derive(Clone)]
pub struct EnqueueAtAction {
    script: Script,
    queue: Queue,
}

impl EnqueueAtAction {
    pub fn new(queue: Queue) -> Self {
        Self {
            script: Script::new(crate::lua::ENQUEUE_AT),
            queue,
        }
    }

    pub fn prepare_invoke<J: Job>(&self, job: &J, run_at: i64) -> YqResult<ScriptInvocation> {
        let mut invoke = self.script.prepare_invoke();
        invoke
            .key(self.queue.mid_seq_key.as_str())
            .key(self.queue.messages_key.as_str())
            .key(self.queue.schedule_key.as_str());

        let job_data = encode_job(job)?;
        invoke.arg(&job_data).arg(run_at);

        Ok(invoke)
    }
}

#[derive(Debug)]
pub enum EnqueueAtStatus {
    Added(i64),
    Unknown(String),
}

impl FromRedisValue for EnqueueAtStatus {
    fn from_redis_value(v: &redis::Value) -> RedisResult<Self> {
        match v {
            redis::Value::Bulk(bulk) => EnqueueAtStatus::try_from(bulk.as_slice()),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "invalid dequeue status - invalid value type",
                format!("{v:?}"),
            ))),
        }
    }
}

impl TryFrom<&[redis::Value]> for EnqueueAtStatus {
    type Error = redis::RedisError;

    fn try_from(values: &[redis::Value]) -> Result<Self, Self::Error> {
        let mut iter = values.iter();
        let action =
            read_redis_value_as_str(iter.next(), "invalid enqueue at status - invalid action")?;

        let status = match action.as_ref() {
            "added" => {
                let mid = read_redis_value_as_int(
                    iter.next(),
                    "invalid enqueue at status - invalid mid",
                )?;
                EnqueueAtStatus::Added(mid)
            }
            _ => EnqueueAtStatus::Unknown(format!("{values:?}")),
        };

        Ok(status)
    }
}
