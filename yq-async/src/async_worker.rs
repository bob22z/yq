use crate::async_job::{AsyncJob, AsyncJobFns};
use redis::{aio::ConnectionManager, AsyncCommands, Client, RedisResult};
use std::sync::Arc;
use yq::{
    DequeueAction, DequeueSleep, DequeueStatus, FinishAction, Queue, SleepOnAction, YqError,
    YqResult,
};

pub struct AsyncWorker<S> {
    connection_manager: ConnectionManager,
    queue: Queue,
    dequeue_action: DequeueAction,
    finish_action: FinishAction,
    sleep_on_action: SleepOnAction,
    async_job_fns: AsyncJobFns<S>,
    state: S,
}

impl<S> AsyncWorker<S>
where
    S: Send + Sync + Clone + 'static,
{
    pub async fn new(redis_url: &str, queue: Queue, state: S) -> YqResult<Self> {
        let client = Client::open(redis_url).map_err(YqError::CreateRedisClient)?;
        let connection_manager = client
            .get_tokio_connection_manager()
            .await
            .map_err(YqError::GetRedisConn)?;

        Ok(Self {
            connection_manager,
            queue: queue.clone(),
            dequeue_action: DequeueAction::new(queue.clone()),
            finish_action: FinishAction::new(queue.clone()),
            sleep_on_action: SleepOnAction::new(queue),
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
                    job_data.execute_async(mid, state).await
                })
            }),
        )?;
        Ok(self)
    }

    async fn sleep(&mut self, dequeue_sleep: DequeueSleep) {
        let r: RedisResult<Option<String>> = self
            .sleep_on_action
            .prepare_invoke(dequeue_sleep)
            .query_async(&mut self.connection_manager)
            .await;

        if let Err(err) = r {
            tracing::error!("worker sleep ERROR: {}", err.to_string());
        }
    }

    pub async fn run(mut self) -> YqResult<()> {
        loop {
            let now = time::OffsetDateTime::now_utc();

            let dequeue_status: RedisResult<DequeueStatus> = self
                .dequeue_action
                .prepare_invoke(now.unix_timestamp())
                .invoke_async(&mut self.connection_manager)
                .await;

            let dequeue_status = match dequeue_status {
                Ok(dequeue_status) => dequeue_status,
                Err(err) => {
                    tracing::error!("dequeue_job ERROR: {err:?}");
                    self.sleep(DequeueSleep::default()).await;
                    continue;
                }
            };

            tracing::trace!("{:?}", &dequeue_status);

            match dequeue_status {
                DequeueStatus::Sleep(dequeue_sleep) => {
                    self.sleep(dequeue_sleep).await;
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
                            let r: RedisResult<i64> = self
                                .finish_action
                                .prepare_invoke(dequeue_handle.mid)
                                .query_async(&mut self.connection_manager)
                                .await;

                            if let Err(err) = r {
                                tracing::error!(
                                    "error when finish_job: {} - {}, {:?}",
                                    &self.queue.queue_name,
                                    dequeue_handle.mid,
                                    err
                                );
                            }
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
