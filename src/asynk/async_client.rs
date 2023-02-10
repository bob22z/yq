use crate::error::{YqError, YqResult};
use crate::queue::{EnqueueStatus, Queue};
use crate::{lua, Job};
use redis::aio::ConnectionManager;

pub struct AsyncClient {
    connection_manager: ConnectionManager,
    enqueue_job: lua::EnqueueJob,
}

impl AsyncClient {
    pub async fn new(redis_ur: &str) -> YqResult<AsyncClient> {
        let client = redis::Client::open(redis_ur).map_err(YqError::CreateRedisClient)?;
        let connection_manager = client
            .get_tokio_connection_manager()
            .await
            .map_err(YqError::GetRedisConn)?;

        Ok(Self {
            connection_manager,
            enqueue_job: lua::EnqueueJob::new(Queue::default()),
        })
    }

    pub async fn schedule<J: Job>(&mut self, job: &J) -> YqResult<i64> {
        let enqueue_status: EnqueueStatus = self
            .enqueue_job
            .invoke_async(job, &mut self.connection_manager)
            .await?;

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
