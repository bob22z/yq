use crate::error::{YqError, YqResult};
use crate::lua;
use crate::queue::{DequeueStatus, Queue};
use crate::sync::sync_job::{SyncJob, SyncJobFns};
use redis::{Client, Commands};
use std::sync::Arc;

pub struct SyncWorker<S> {
    client: Client,
    queue: Queue,
    dequeue_job: lua::DequeueJob,
    finish_job: lua::FinishJob,
    sleep_on_job: lua::SleepOnJob,
    sync_job_fns: SyncJobFns<S>,
    state: S,
}

impl<S> SyncWorker<S>
where
    S: Send + Sync + Clone + 'static,
{
    pub fn new(redis_ur: &str, state: S) -> YqResult<Self> {
        let client = Client::open(redis_ur).map_err(YqError::CreateRedisClient)?;

        let queue = Queue::default();

        Ok(Self {
            client,
            queue: queue.clone(),
            dequeue_job: lua::DequeueJob::new(queue.clone()),
            finish_job: lua::FinishJob::new(queue.clone()),
            sleep_on_job: lua::SleepOnJob::new(queue),
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

    pub fn run(mut self) -> YqResult<()> {
        loop {
            let now = time::OffsetDateTime::now_utc();

            let dequeue_status = self.dequeue_job.invoke(now, &mut self.client)?;

            tracing::trace!("{:?}", &dequeue_status);

            match dequeue_status {
                DequeueStatus::Sleep(dequeue_sleep) => {
                    self.sleep_on_job.invoke(dequeue_sleep, &mut self.client);
                }
                DequeueStatus::Handle(dequeue_handle) => {
                    match self.sync_job_fns.handle(
                        dequeue_handle.mid,
                        dequeue_handle.mcontent,
                        self.state.clone(),
                    ) {
                        Ok(_) => {
                            self.finish_job.invoke(dequeue_handle.mid, &mut self.client);
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
