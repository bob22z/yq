use yq::Queue;
use yq_async::AsyncWorker;
use yq_async_examples::{HelloAsyncJob, HelloAsyncState};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let state = HelloAsyncState::new("redis://127.0.0.1").await?;
    let queue = Queue::default();

    let worker = AsyncWorker::new("redis://127.0.0.1/", queue, state)
        .await?
        .reg_job::<HelloAsyncJob>()?;

    worker.run().await.map_err(Into::into)
}

fn init_tracing() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter("yq=TRACE")
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
