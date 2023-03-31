use yq_scheduler::Scheduler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let redis_url = try_get_redis_url()?;
    let scheduler = Scheduler::new(&redis_url).await?;
    scheduler.run().await?;

    Ok(())
}

fn try_get_redis_url() -> Result<String, Box<dyn std::error::Error>> {
    if let Ok(redis_url) = std::env::var("YQ_REDIS_URL") {
        return Ok(redis_url);
    }

    let mut args = std::env::args();
    args.next();

    args.next()
        .ok_or_else(|| "Could not get YQ_REDIS_URL from ENV or ARGS".into())
}

fn init_tracing() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
