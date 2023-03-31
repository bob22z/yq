use async_trait::async_trait;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use yq::{decode_job, Job, JobType, YqError, YqResult, YqRunJobError};

#[async_trait]
pub trait AsyncJob: Job + 'static + Send {
    async fn execute_async(self, mid: i64, state: Self::State) -> Result<(), String>;
}

type AsyncJobFn<S> = Arc<
    dyn Fn(i64, String, S) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
        + Send
        + Sync,
>;

pub(crate) struct AsyncJobFns<S>(HashMap<JobType, AsyncJobFn<S>>);

impl<S> AsyncJobFns<S> {
    pub(crate) fn new() -> AsyncJobFns<S> {
        AsyncJobFns::<S>(HashMap::default())
    }

    pub(crate) fn reg_job(&mut self, job_type: JobType, job_fn: AsyncJobFn<S>) -> YqResult<()> {
        if self.0.insert(job_type.clone(), job_fn).is_some() {
            Err(YqError::DupJobType(job_type))
        } else {
            Ok(())
        }
    }

    pub(crate) async fn handle(&self, mid: i64, mcontent: String, state: S) -> YqResult<()> {
        let (job_type, job_data) = decode_job(&mcontent)?;

        let job_fn = match self.0.get(job_type) {
            Some(job_fn) => job_fn,
            None => {
                return Err(YqError::JobTypeMissing(JobType::from(job_type.to_string())));
            }
        };

        job_fn(mid, job_data.to_string(), state)
            .await
            .map_err(|error| YqError::RunJobError(YqRunJobError::new(job_data.to_string(), error)))
    }
}
