use crate::helper::{read_redis_value_as_int, read_redis_value_as_str};
use crate::Queue;
use redis::{FromRedisValue, RedisResult, Script, ScriptInvocation};

pub struct DequeueAtAction {
    script: Script,
    queue: Queue,
}

impl DequeueAtAction {
    pub fn new(queue: Queue) -> Self {
        Self {
            script: Script::new(crate::lua::DEQUEUE_AT),
            queue,
        }
    }

    pub fn prepare_invoke(&self, run_at: i64) -> ScriptInvocation {
        let mut invoke = self.script.prepare_invoke();
        invoke
            .key(self.queue.mids_ready_key.as_str())
            .key(self.queue.mid_circle_key.as_str())
            .key(self.queue.schedule_key.as_str());

        invoke.arg(run_at);

        invoke
    }
}

#[derive(Debug)]
pub enum DequeueAtStatus {
    Dequeued(i64),
    NoJob,
    Unknown(String),
}

impl FromRedisValue for DequeueAtStatus {
    fn from_redis_value(v: &redis::Value) -> RedisResult<Self> {
        match v {
            redis::Value::Bulk(bulk) => DequeueAtStatus::try_from(bulk.as_slice()),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "invalid dequeue at status - invalid value type",
                format!("{v:?}"),
            ))),
        }
    }
}

impl TryFrom<&[redis::Value]> for DequeueAtStatus {
    type Error = redis::RedisError;

    fn try_from(values: &[redis::Value]) -> Result<Self, Self::Error> {
        let mut iter = values.iter();
        let action =
            read_redis_value_as_str(iter.next(), "invalid dequeue at status - invalid action")?;

        let status = match action.as_ref() {
            "dequeued" => {
                let count = read_redis_value_as_int(
                    iter.next(),
                    "invalid dequeue at status - sleep - invalid count",
                )?;
                DequeueAtStatus::Dequeued(count)
            }
            "no-job" => DequeueAtStatus::NoJob,
            _ => DequeueAtStatus::Unknown(format!("{values:?}")),
        };

        Ok(status)
    }
}
