use redis::Commands;
use serde::{Deserialize, Serialize};
use yq::sync::{SyncJob, SyncWorker};
use yq::{Job, JobType};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter("yq=TRACE")
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let redis_client = redis::Client::open("redis://127.0.0.1")?;
    let state = HelloState {
        redis_client,
        count: 2,
    };
    let worker = SyncWorker::new("redis://127.0.0.1/", state)?.reg_job::<HelloJob>()?;

    worker.run().map_err(Into::into)
}

#[derive(Serialize, Deserialize, Debug)]
struct HelloJob {
    id: i64,
    name: String,
}

#[derive(Clone)]
struct HelloState {
    redis_client: redis::Client,
    count: i64,
}

impl Job for HelloJob {
    const JOB_TYPE: JobType = JobType::Borrowed("HelloJob");
    type State = HelloState;
}

impl SyncJob for HelloJob {
    fn execute(self, mid: i64, mut state: Self::State) -> Result<(), String> {
        let values: Vec<String> = state
            .redis_client
            .keys("*")
            .map_err(|err| err.to_string())?;
        dbg!(mid, self, state.count, values);
        Ok(())
    }
}
