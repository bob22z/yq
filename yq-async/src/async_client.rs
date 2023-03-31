use redis::aio::ConnectionManager;
use yq::{
    EnqueueAction, EnqueueAtAction, EnqueueAtStatus, EnqueueStatus, Job, Queue, YqError, YqResult,
};

pub struct AsyncClient {
    connection_manager: ConnectionManager,
    enqueue_action: EnqueueAction,
    enqueue_at_action: EnqueueAtAction,
}

impl AsyncClient {
    pub async fn new(redis_ur: &str, queue: Queue) -> YqResult<AsyncClient> {
        let client = redis::Client::open(redis_ur).map_err(YqError::CreateRedisClient)?;
        let connection_manager = client
            .get_tokio_connection_manager()
            .await
            .map_err(YqError::GetRedisConn)?;

        Ok(Self {
            connection_manager,
            enqueue_action: EnqueueAction::new(queue.clone()),
            enqueue_at_action: EnqueueAtAction::new(queue),
        })
    }

    pub async fn schedule<J: Job>(&mut self, job: &J) -> YqResult<i64> {
        let enqueue_status: EnqueueStatus = self
            .enqueue_action
            .prepare_invoke(job)?
            .invoke_async(&mut self.connection_manager)
            .await
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

    pub async fn schedule_at<J: Job>(&mut self, job: &J, run_at: i64) -> YqResult<i64> {
        let enqueue_at_status: EnqueueAtStatus = self
            .enqueue_at_action
            .prepare_invoke(job, run_at)?
            .invoke_async(&mut self.connection_manager)
            .await
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
