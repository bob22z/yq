use crate::error::{YqError, YqResult};
use crate::queue::{EnqueueStatus, Queue};
use crate::{lua, Job};
use redis::Client;

pub struct SyncClient {
    client: Client,
    enqueue_job: lua::EnqueueJob,
}

impl SyncClient {
    pub fn new(redis_ur: &str) -> YqResult<SyncClient> {
        let client = Client::open(redis_ur).map_err(YqError::CreateRedisClient)?;
        let queue = Queue::default();

        Ok(Self {
            client,
            enqueue_job: lua::EnqueueJob::new(queue),
        })
    }

    pub fn schedule<J: Job>(&mut self, job: &J) -> YqResult<i64> {
        let enqueue_status: EnqueueStatus = self.enqueue_job.invoke(job, &mut self.client)?;

        match enqueue_status {
            EnqueueStatus::Added(added) => Ok(added.mid),
            EnqueueStatus::Unknown(err) => Err(YqError::Enqueue(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "enqueue error",
                err,
            )))),
        }
    }
}
