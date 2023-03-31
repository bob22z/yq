use yq::Queue;
use yq_sync::SyncClient;
use yq_sync_example::HelloSyncJob;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = Queue::default();
    let mut client = SyncClient::new("redis://127.0.0.1/", queue)?;

    add_jobs(&mut client)?;

    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    add_jobs_at(&mut client, now)?;

    Ok(())
}

fn add_jobs(client: &mut SyncClient) -> Result<(), Box<dyn std::error::Error>> {
    let hello_job1 = HelloSyncJob::new(1, "job-1".into());
    client.schedule(&hello_job1)?;

    let hello_job2 = HelloSyncJob::new(2, "job-2".into());
    client.schedule(&hello_job2)?;

    let hello_job3 = HelloSyncJob::new(3, "job-3".into());
    client.schedule(&hello_job3)?;
    Ok(())
}

fn add_jobs_at(client: &mut SyncClient, run_at: i64) -> Result<(), Box<dyn std::error::Error>> {
    let hello_job10 = HelloSyncJob::new(10, "bob-10".into());
    client.schedule_at(&hello_job10, run_at)?;
    let hello_job11 = HelloSyncJob::new(11, "bob-11".into());
    client.schedule_at(&hello_job11, run_at)?;

    let next_run_at = run_at + 10;
    let hello_job12 = HelloSyncJob::new(12, "bob-12".into());
    client.schedule_at(&hello_job12, next_run_at)?;

    let hello_job13 = HelloSyncJob::new(13, "bob-12".into());
    client.schedule_at(&hello_job13, next_run_at)?;

    let hello_job14 = HelloSyncJob::new(14, "bob-14".into());
    client.schedule_at(&hello_job14, next_run_at)?;
    Ok(())
}
