use crate::error::{YqError, YqResult};
use crate::Job;

pub(crate) fn decode_job(mcontent: &str) -> YqResult<(&str, &str)> {
    let (job_type_len, rest_mcontent) = match mcontent.split_once(':') {
        Some(r) => r,
        None => {
            return Err(YqError::InvalidJobData(mcontent.into()));
        }
    };

    let job_type_len: usize = job_type_len
        .parse::<usize>()
        .map_err(|_err| YqError::InvalidJobData(mcontent.into()))?;

    Ok(rest_mcontent.split_at(job_type_len))
}

pub(crate) fn encode_job<J: Job>(job: &J) -> YqResult<String> {
    let job_str = serde_json::to_string(job).map_err(YqError::SerializeJob)?;
    Ok(format!("{}:{}{}", J::JOB_TYPE.len(), J::JOB_TYPE, job_str))
}
