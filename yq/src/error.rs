use crate::JobType;
use thiserror::Error;

pub type YqResult<T> = Result<T, YqError>;

#[derive(Error, Debug)]
pub enum YqError {
    #[error("CreateRedisClient")]
    CreateRedisClient(redis::RedisError),
    #[error("GetRedisConn")]
    GetRedisConn(redis::RedisError),
    #[error("Dequeue")]
    Dequeue(redis::RedisError),
    #[error("DequeueAt")]
    DequeueAt(redis::RedisError),
    #[error("Enqueue")]
    Enqueue(redis::RedisError),
    #[error("EnqueueAt")]
    EnqueueAt(redis::RedisError),
    #[error("SerializeJob")]
    SerializeJob(serde_json::Error),
    #[error("DupJobHandler")]
    DupJobType(JobType),
    #[error("JobTypeMissing")]
    JobTypeMissing(JobType),
    #[error("JobTypeMissing")]
    InvalidJobData(String),
    #[error("RunJobError")]
    RunJobError(YqRunJobError),
    #[error("FailJobError")]
    FailJobError(redis::RedisError),
}

#[derive(Debug)]
pub struct YqRunJobError {
    pub job_data: String,
    pub error: String,
}

impl YqRunJobError {
    pub fn new(job_data: String, error: String) -> Self {
        Self { job_data, error }
    }
}
