use yq::Queue;
use yq_sync::SyncWorker;
use yq_sync_example::{HelloSyncJob, HelloSyncState};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let state = HelloSyncState::new("redis://127.0.0.1")?;
    let queue = Queue::default();

    let worker = SyncWorker::new("redis://127.0.0.1/", queue, state)?.reg_job::<HelloSyncJob>()?;

    worker.run().map_err(Into::into)
}

fn init_tracing() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter("yq=TRACE")
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
