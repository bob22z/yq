use redis::aio::ConnectionManager;
use redis::Client;
use yq::{DequeueAtAction, DequeueAtStatus, Queue, YqError, YqResult};

pub struct Scheduler {
    connection_manager: ConnectionManager,
    dequeue_at_action: DequeueAtAction,
}

impl Scheduler {
    pub async fn new(redis_url: &str) -> YqResult<Self> {
        let client = Client::open(redis_url).map_err(YqError::CreateRedisClient)?;
        let connection_manager = client
            .get_tokio_connection_manager()
            .await
            .map_err(YqError::GetRedisConn)?;

        let queue = Queue::default();

        Ok(Self {
            connection_manager,
            dequeue_at_action: DequeueAtAction::new(queue),
        })
    }

    pub async fn run(mut self) -> YqResult<()> {
        loop {
            if let Err(err) = self.dequeue_loop().await {
                tracing::error!("dequeue_at_loop ERROR: {err:?}");
                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
            }
        }
    }

    async fn dequeue_loop(&mut self) -> YqResult<()> {
        loop {
            let now = time::OffsetDateTime::now_utc();

            let dequeue_at_status: DequeueAtStatus = self
                .dequeue_at_action
                .prepare_invoke(now.unix_timestamp())
                .invoke_async(&mut self.connection_manager)
                .await
                .map_err(YqError::DequeueAt)?;

            match dequeue_at_status {
                DequeueAtStatus::Dequeued(count) => {
                    tracing::trace!("dequeued {count} jobs");
                }
                DequeueAtStatus::NoJob => {
                    tracing::trace!("dequeued no jobs");
                    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                }
                DequeueAtStatus::Unknown(err) => {
                    tracing::error!("dequeued ERROR: {err}");
                    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                }
            }
        }
    }
}
