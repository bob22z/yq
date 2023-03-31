use yq::Queue;
use yq_async::AsyncClient;
use yq_async_examples::HelloAsyncJob;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = Queue::default();
    let mut client = AsyncClient::new("redis://127.0.0.1/", queue).await?;

    add_jobs(&mut client).await?;

    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    add_jobs_at(&mut client, now).await?;

    Ok(())
}

async fn add_jobs(client: &mut AsyncClient) -> Result<(), Box<dyn std::error::Error>> {
    let hello_job1 = HelloAsyncJob::new(1, "job-1".into());
    client.schedule(&hello_job1).await?;

    let hello_job2 = HelloAsyncJob::new(2, "job-2".into());
    client.schedule(&hello_job2).await?;

    let hello_job3 = HelloAsyncJob::new(3, "job-3".into());
    client.schedule(&hello_job3).await?;
    Ok(())
}

async fn add_jobs_at(
    client: &mut AsyncClient,
    run_at: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let hello_job10 = HelloAsyncJob::new(10, "bob-10".into());
    client.schedule_at(&hello_job10, run_at).await?;
    let hello_job11 = HelloAsyncJob::new(11, "bob-11".into());
    client.schedule_at(&hello_job11, run_at).await?;

    let next_run_at = run_at + 10;
    let hello_job12 = HelloAsyncJob::new(12, "bob-12".into());
    client.schedule_at(&hello_job12, next_run_at).await?;

    let hello_job13 = HelloAsyncJob::new(13, "bob-12".into());
    client.schedule_at(&hello_job13, next_run_at).await?;

    let hello_job14 = HelloAsyncJob::new(14, "bob-14".into());
    client.schedule_at(&hello_job14, next_run_at).await?;
    Ok(())
}
