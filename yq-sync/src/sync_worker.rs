use crate::sync_job::{SyncJob, SyncJobFns};
use redis::{Client, Commands, RedisResult};
use std::sync::Arc;
use yq::{
    DequeueAction, DequeueSleep, DequeueStatus, FinishAction, Queue, SleepOnAction, YqError,
    YqResult,
};

pub struct SyncWorker<S> {
    client: Client,
    queue: Queue,
    dequeue_action: DequeueAction,
    finish_action: FinishAction,
    sleep_on_action: SleepOnAction,
    sync_job_fns: SyncJobFns<S>,
    state: S,
}

impl<S> SyncWorker<S>
where
    S: Send + Sync + Clone + 'static,
{
    pub fn new(redis_ur: &str, queue: Queue, state: S) -> YqResult<Self> {
        let client = Client::open(redis_ur).map_err(YqError::CreateRedisClient)?;

        Ok(Self {
            client,
            queue: queue.clone(),
            dequeue_action: DequeueAction::new(queue.clone()),
            finish_action: FinishAction::new(queue.clone()),
            sleep_on_action: SleepOnAction::new(queue),
            sync_job_fns: SyncJobFns::new(),
            state,
        })
    }

    pub fn reg_job<J: SyncJob<State = S>>(mut self) -> YqResult<Self> {
        let job_type = J::JOB_TYPE;

        self.sync_job_fns.reg_job(
            job_type,
            Arc::new(|mid, job_content, state| {
                let job_data: J =
                    serde_json::from_str(&job_content).map_err(|err| err.to_string())?;
                job_data.execute(mid, state)
            }),
        )?;
        Ok(self)
    }

    fn sleep(&mut self, dequeue_sleep: DequeueSleep) {
        let r: RedisResult<Option<String>> = self
            .sleep_on_action
            .prepare_invoke(dequeue_sleep)
            .query(&mut self.client);

        if let Err(err) = r {
            tracing::error!("worker sleep ERROR: {}", err.to_string());
        }
    }

    pub fn run(mut self) -> YqResult<()> {
        loop {
            let now = time::OffsetDateTime::now_utc();

            let dequeue_status: RedisResult<DequeueStatus> = self
                .dequeue_action
                .prepare_invoke(now.unix_timestamp())
                .invoke(&mut self.client);

            let dequeue_status = match dequeue_status {
                Ok(dequeue_status) => dequeue_status,
                Err(err) => {
                    tracing::error!("dequeue_job ERROR: {err:?}");
                    self.sleep(DequeueSleep::default());
                    continue;
                }
            };
            tracing::trace!("{:?}", &dequeue_status);

            match dequeue_status {
                DequeueStatus::Sleep(dequeue_sleep) => {
                    self.sleep(dequeue_sleep);
                }
                DequeueStatus::Handle(dequeue_handle) => {
                    match self.sync_job_fns.handle(
                        dequeue_handle.mid,
                        dequeue_handle.mcontent,
                        self.state.clone(),
                    ) {
                        Ok(_) => {
                            let r: RedisResult<i64> = self
                                .finish_action
                                .prepare_invoke(dequeue_handle.mid)
                                .query(&mut self.client);

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
                            if let Err(err) = self.fail_job(dequeue_handle.mid, err) {
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

    fn fail_job(&mut self, job_id: i64, err: YqError) -> YqResult<()> {
        match err {
            YqError::RunJobError(run_job_error) => {
                self.client
                    .hset(
                        self.queue.err_messages_key.as_str(),
                        job_id,
                        run_job_error.job_data,
                    )
                    .map_err(YqError::FailJobError)?;

                self.client
                    .hset(self.queue.err_key.as_str(), job_id, run_job_error.error)
                    .map_err(YqError::FailJobError)?;
            }
            other => {
                self.client
                    .hset(self.queue.err_key.as_str(), job_id, other.to_string())
                    .map_err(YqError::FailJobError)?;
            }
        }

        Ok(())
    }
}
