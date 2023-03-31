use crate::error::{YqError, YqResult};
use crate::Job;
use redis::RedisResult;
use std::borrow::Cow;

pub fn decode_job(mcontent: &str) -> YqResult<(&str, &str)> {
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

pub(crate) fn read_redis_value_as_str<'a>(
    v: Option<&'a redis::Value>,
    err_desc: &'static str,
) -> RedisResult<Cow<'a, str>> {
    let v = match v {
        Some(v) => v,
        None => {
            return Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                err_desc,
            )))
        }
    };

    match v {
        redis::Value::Data(d) => Ok(String::from_utf8_lossy(d.as_slice())),
        _ => Err(redis::RedisError::from((
            redis::ErrorKind::ResponseError,
            err_desc,
            format!("{v:?}"),
        ))),
    }
}

pub(crate) fn read_redis_value_as_int(
    v: Option<&redis::Value>,
    err_desc: &'static str,
) -> RedisResult<i64> {
    let v = match v {
        Some(v) => v,
        None => {
            return Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                err_desc,
            )))
        }
    };

    match v {
        redis::Value::Int(i) => Ok(*i),
        _ => Err(redis::RedisError::from((
            redis::ErrorKind::ResponseError,
            err_desc,
            format!("{v:?}"),
        ))),
    }
}
