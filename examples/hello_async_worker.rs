use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use yq::asynk::{AsyncJob, AsyncWorker};
use yq::{Job, JobType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter("yq=TRACE")
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let redis_client = redis::Client::open("redis://127.0.0.1")?;
    let connection_manager = redis_client.get_tokio_connection_manager().await?;

    let state = HelloState {
        connection_manager,
        count: 2,
    };
    let worker = AsyncWorker::new("redis://127.0.0.1/", state)
        .await?
        .reg_job::<HelloJob>()?;

    worker.run().await.map_err(Into::into)
}

#[derive(Serialize, Deserialize, Debug)]
struct HelloJob {
    id: i64,
    name: String,
}

#[derive(Clone)]
struct HelloState {
    connection_manager: ConnectionManager,
    count: i64,
}

impl Job for HelloJob {
    const JOB_TYPE: JobType = JobType::Borrowed("HelloJob");
    type State = HelloState;
}

#[async_trait]
impl AsyncJob for HelloJob {
    async fn execute(self, mid: i64, mut state: Self::State) -> Result<(), String> {
        let values: Vec<String> = state
            .connection_manager
            .keys("*")
            .await
            .map_err(|err| err.to_string())?;
        dbg!(mid, self, state.count, values);
        Ok(())
    }
}
