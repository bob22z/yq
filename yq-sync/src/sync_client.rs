use redis::Client;
use yq::{
    EnqueueAction, EnqueueAtAction, EnqueueAtStatus, EnqueueStatus, Job, Queue, YqError, YqResult,
};

#[derive(Clone)]
pub struct SyncClient {
    client: Client,
    enqueue_action: EnqueueAction,
    enqueue_at_action: EnqueueAtAction,
}

impl SyncClient {
    pub fn new(redis_ur: &str, queue: Queue) -> YqResult<SyncClient> {
        let client = Client::open(redis_ur).map_err(YqError::CreateRedisClient)?;

        Ok(Self {
            client,
            enqueue_action: EnqueueAction::new(queue.clone()),
            enqueue_at_action: EnqueueAtAction::new(queue),
        })
    }

    pub fn schedule<J: Job>(&self, job: &J) -> YqResult<i64> {
        let mut redis_conn = self
            .client
            .get_connection()
            .map_err(YqError::GetRedisConn)?;
        let enqueue_status: EnqueueStatus = self
            .enqueue_action
            .prepare_invoke(job)?
            .invoke(&mut redis_conn)
            .map_err(YqError::Enqueue)?;

        match enqueue_status {
            EnqueueStatus::Added(added) => Ok(added.mid),
            EnqueueStatus::Unknown(err) => Err(YqError::Enqueue(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "enqueue error",
                err,
            )))),
        }
    }

    pub fn schedule_at<J: Job>(&self, job: &J, run_at: i64) -> YqResult<i64> {
        let mut redis_conn = self
            .client
            .get_connection()
            .map_err(YqError::GetRedisConn)?;

        let enqueue_at_status: EnqueueAtStatus = self
            .enqueue_at_action
            .prepare_invoke(job, run_at)?
            .invoke(&mut redis_conn)
            .map_err(YqError::EnqueueAt)?;

        match enqueue_at_status {
            EnqueueAtStatus::Added(mid) => Ok(mid),
            EnqueueAtStatus::Unknown(err) => Err(YqError::Enqueue(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "enqueue error",
                err,
            )))),
        }
    }
}
