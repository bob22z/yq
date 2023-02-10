use crate::error::{RunJobError, YqError, YqResult};
use crate::helper::decode_job;
use crate::{Job, JobType};
use std::collections::HashMap;
use std::sync::Arc;

pub trait SyncJob: Job {
    fn execute(self, mid: i64, state: Self::State) -> Result<(), String>;
}

type SyncJobFn<S> = Arc<dyn Fn(i64, String, S) -> Result<(), String>>;

pub(crate) struct SyncJobFns<S>(HashMap<JobType, SyncJobFn<S>>);

impl<S> SyncJobFns<S> {
    pub(crate) fn new() -> SyncJobFns<S> {
        SyncJobFns::<S>(HashMap::default())
    }

    pub(crate) fn reg_job(&mut self, job_type: JobType, job_fn: SyncJobFn<S>) -> YqResult<()> {
        if self.0.insert(job_type.clone(), job_fn).is_some() {
            Err(YqError::DupJobType(job_type))
        } else {
            Ok(())
        }
    }

    pub(crate) fn handle(&self, mid: i64, mcontent: String, state: S) -> YqResult<()> {
        let (job_type, job_data) = decode_job(&mcontent)?;

        let job_fn = match self.0.get(job_type) {
            Some(job_fn) => job_fn,
            None => {
                return Err(YqError::JobTypeMissing(JobType::from(job_type.to_string())));
            }
        };

        job_fn(mid, job_data.to_string(), state).map_err(|error| {
            YqError::RunJobError(RunJobError {
                job_data: job_data.to_string(),
                error,
            })
        })
    }
}
