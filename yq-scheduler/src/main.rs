use yq_scheduler::Scheduler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter("yq=TRACE,yq_scheduler=TRACE")
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let scheduler = Scheduler::new("redis://127.0.0.1/").await?;
    scheduler.run().await?;

    Ok(())
}
