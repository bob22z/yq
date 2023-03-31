use async_trait::async_trait;
use redis::{aio::ConnectionManager, AsyncCommands};
use serde::{Deserialize, Serialize};
use yq::{Job, JobType};
use yq_async::AsyncJob;

#[derive(Serialize, Deserialize, Debug)]
pub struct HelloAsyncJob {
    id: i64,
    name: String,
}

impl HelloAsyncJob {
    pub fn new(id: i64, name: String) -> Self {
        Self { id, name }
    }
}

#[derive(Clone)]
pub struct HelloAsyncState {
    connection_manager: ConnectionManager,
}

impl HelloAsyncState {
    pub async fn new(redis_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let redis_client = redis::Client::open(redis_url)?;
        let connection_manager = redis_client.get_tokio_connection_manager().await?;
        Ok(Self { connection_manager })
    }
}

impl Job for HelloAsyncJob {
    const JOB_TYPE: JobType = JobType::Borrowed("HelloAsyncJob");
    type State = HelloAsyncState;
}

#[async_trait]
impl AsyncJob for HelloAsyncJob {
    async fn execute_async(self, mid: i64, mut state: Self::State) -> Result<(), String> {
        let keys: Vec<String> = state
            .connection_manager
            .keys("*")
            .await
            .map_err(|err| err.to_string())?;
        println!(
            "HelloAsyncJob.execute_async: mid={mid}, keys={}",
            keys.len()
        );
        Ok(())
    }
}
