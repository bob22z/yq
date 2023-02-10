use crate::asynk::async_job::{AsyncJob, AsyncJobFns};
use crate::error::{YqError, YqResult};
use crate::lua;
use crate::queue::{DequeueStatus, Queue};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client};
use std::sync::Arc;

pub struct AsyncWorker<S> {
    connection_manager: ConnectionManager,
    queue: Queue,
    dequeue_job: lua::DequeueJob,
    finish_job: lua::FinishJob,
    sleep_on_job: lua::SleepOnJob,
    async_job_fns: AsyncJobFns<S>,
    state: S,
}

impl<S> AsyncWorker<S>
where
    S: Send + Sync + Clone + 'static,
{
    pub async fn new(redis_ur: &str, state: S) -> YqResult<Self> {
        let client = Client::open(redis_ur).map_err(YqError::CreateRedisClient)?;
        let connection_manager = client
            .get_tokio_connection_manager()
            .await
            .map_err(YqError::GetRedisConn)?;

        let queue = Queue::default();
        Ok(Self {
            connection_manager,
            queue: queue.clone(),
            dequeue_job: lua::DequeueJob::new(queue.clone()),
            finish_job: lua::FinishJob::new(queue.clone()),
            sleep_on_job: lua::SleepOnJob::new(queue),
            async_job_fns: AsyncJobFns::new(),
            state,
        })
    }

    pub fn reg_job<J: AsyncJob<State = S>>(mut self) -> YqResult<Self> {
        let job_type = J::JOB_TYPE;

        self.async_job_fns.reg_job(
            job_type,
            Arc::new(|mid, job_content, state| {
                Box::pin(async move {
                    let job_data: J =
                        serde_json::from_str(&job_content).map_err(|err| err.to_string())?;
                    job_data.execute(mid, state).await
                })
            }),
        )?;
        Ok(self)
    }

    pub async fn run(mut self) -> YqResult<()> {
        loop {
            let now = time::OffsetDateTime::now_utc();

            let dequeue_status = self
                .dequeue_job
                .invoke_async(now, &mut self.connection_manager)
                .await?;

            tracing::trace!("{:?}", &dequeue_status);

            match dequeue_status {
                DequeueStatus::Sleep(dequeue_sleep) => {
                    self.sleep_on_job
                        .invoke_async(dequeue_sleep, &mut self.connection_manager)
                        .await;
                }
                DequeueStatus::Handle(dequeue_handle) => {
                    match self
                        .async_job_fns
                        .handle(
                            dequeue_handle.mid,
                            dequeue_handle.mcontent,
                            self.state.clone(),
                        )
                        .await
                    {
                        Ok(_) => {
                            self.finish_job
                                .invoke_async(dequeue_handle.mid, &mut self.connection_manager)
                                .await;
                        }
                        Err(err) => {
                            if let Err(err) = self.fail_job(dequeue_handle.mid, err).await {
                                tracing::error!("{:?}", err);
                            }
                        }
                    }
                }
                DequeueStatus::Skip(_dequeue_skip) => {
                    // skip and continue
                }
                DequeueStatus::Unknown(s) => panic!("{}", s),
            }
        }
    }

    async fn fail_job(&mut self, job_id: i64, err: YqError) -> YqResult<()> {
        match err {
            YqError::RunJobError(run_job_error) => {
                self.connection_manager
                    .hset(
                        self.queue.err_messages_key.as_str(),
                        job_id,
                        run_job_error.job_data,
                    )
                    .await
                    .map_err(YqError::FailJobError)?;
                self.connection_manager
                    .hset(self.queue.err_key.as_str(), job_id, run_job_error.error)
                    .await
                    .map_err(YqError::FailJobError)?;
            }
            other => {
                self.connection_manager
                    .hset(self.queue.err_key.as_str(), job_id, other.to_string())
                    .await
                    .map_err(YqError::FailJobError)?;
            }
        }
        Ok(())
    }
}
