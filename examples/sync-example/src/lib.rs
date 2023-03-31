use redis::Commands;
use serde::{Deserialize, Serialize};
use yq::{Job, JobType};
use yq_sync::SyncJob;

#[derive(Serialize, Deserialize, Debug)]
pub struct HelloSyncJob {
    id: i64,
    name: String,
}

impl HelloSyncJob {
    pub fn new(id: i64, name: String) -> Self {
        Self { id, name }
    }
}

#[derive(Clone)]
pub struct HelloSyncState {
    redis_client: redis::Client,
}

impl HelloSyncState {
    pub fn new(redis_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let redis_client = redis::Client::open(redis_url)?;

        Ok(Self { redis_client })
    }
}

impl Job for HelloSyncJob {
    const JOB_TYPE: JobType = JobType::Borrowed("HelloSyncJob");
    type State = HelloSyncState;
}

impl SyncJob for HelloSyncJob {
    fn execute(self, mid: i64, mut state: Self::State) -> Result<(), String> {
        let keys: Vec<String> = state
            .redis_client
            .keys("*")
            .map_err(|err| err.to_string())?;
        println!("HelloSyncJob.execute: mid={mid}, keys={}", keys.len());
        Ok(())
    }
}
