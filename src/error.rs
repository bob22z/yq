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
    #[error("Enqueue")]
    Enqueue(redis::RedisError),
    #[error("SerializeJob")]
    SerializeJob(serde_json::Error),
    #[error("DupJobHandler")]
    DupJobType(JobType),
    #[error("JobTypeMissing")]
    JobTypeMissing(JobType),
    #[error("JobTypeMissing")]
    InvalidJobData(String),
    #[error("RunJobError")]
    RunJobError(RunJobError),
    #[error("FailJobError")]
    FailJobError(redis::RedisError),
}

#[derive(Debug)]
pub struct RunJobError {
    pub(crate) job_data: String,
    pub(crate) error: String,
}
