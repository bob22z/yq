use crate::error::{YqError, YqResult};
use crate::helper::encode_job;
use crate::queue::{DequeueSleep, DequeueStatus, EnqueueStatus, Queue};
use crate::Job;
use redis::{Script, ScriptInvocation};
use time::OffsetDateTime;

const ENQUEUE: &str = include_str!("enqueue.lua");
const DEQUEUE: &str = include_str!("dequeue.lua");

pub(crate) struct EnqueueJob {
    script: Script,
    queue: Queue,
}

impl EnqueueJob {
    pub(crate) fn new(queue: Queue) -> Self {
        Self {
            script: Script::new(ENQUEUE),
            queue,
        }
    }

    pub(crate) async fn invoke_async<J: Job, C: redis::aio::ConnectionLike>(
        &self,
        job: &J,
        conn: &mut C,
    ) -> YqResult<EnqueueStatus> {
        self.prepare_invoke(job)?
            .invoke_async(conn)
            .await
            .map_err(YqError::Enqueue)
    }

    pub(crate) fn invoke<J: Job>(
        &self,
        job: &J,
        conn: &mut dyn redis::ConnectionLike,
    ) -> YqResult<EnqueueStatus> {
        self.prepare_invoke(job)?
            .invoke(conn)
            .map_err(YqError::Enqueue)
    }

    fn prepare_invoke<J: Job>(&self, job: &J) -> YqResult<ScriptInvocation> {
        let mut invoke = self.script.prepare_invoke();
        invoke
            .key(self.queue.mid_seq_key.as_str())
            .key(self.queue.messages_key.as_str())
            .key(self.queue.lock_times_key.as_str())
            .key(self.queue.mids_ready_key.as_str())
            .key(self.queue.mid_circle_key.as_str())
            .key(self.queue.isleep_a_key.as_str())
            .key(self.queue.isleep_b_key.as_str());

        let job_data = encode_job(job)?;
        invoke.arg(&job_data).arg(J::LOCK_MS);

        Ok(invoke)
    }
}

pub(crate) struct DequeueJob {
    script: Script,
    queue: Queue,
}

impl DequeueJob {
    pub(crate) fn new(queue: Queue) -> Self {
        Self {
            script: Script::new(DEQUEUE),
            queue,
        }
    }

    pub(crate) async fn invoke_async<C: redis::aio::ConnectionLike>(
        &self,
        now: OffsetDateTime,
        conn: &mut C,
    ) -> YqResult<DequeueStatus> {
        self.prepare_invoke(now)?
            .invoke_async(conn)
            .await
            .map_err(YqError::Enqueue)
    }

    pub(crate) fn invoke(
        &self,
        now: OffsetDateTime,
        conn: &mut dyn redis::ConnectionLike,
    ) -> YqResult<DequeueStatus> {
        self.prepare_invoke(now)?
            .invoke(conn)
            .map_err(YqError::Enqueue)
    }

    fn prepare_invoke(&self, now: OffsetDateTime) -> YqResult<ScriptInvocation> {
        let mut invoke = self.script.prepare_invoke();
        invoke
            .key(self.queue.messages_key.as_str())
            .key(self.queue.lock_times_key.as_str())
            .key(self.queue.locks_key.as_str())
            .key(self.queue.done_key.as_str())
            .key(self.queue.mids_ready_key.as_str())
            .key(self.queue.mid_circle_key.as_str())
            .key(self.queue.ndry_runs_key.as_str())
            .key(self.queue.isleep_b_key.as_str());

        invoke
            .arg(now.unix_timestamp() * 1000)
            .arg(self.queue.default_lock_ms);

        Ok(invoke)
    }
}

pub(crate) struct FinishJob {
    queue: Queue,
}

impl FinishJob {
    pub(crate) fn new(queue: Queue) -> Self {
        Self { queue }
    }

    pub(crate) async fn invoke_async<C: redis::aio::ConnectionLike>(
        &self,
        job_id: i64,
        conn: &mut C,
    ) {
        let r: redis::RedisResult<i64> = self.prepare_invoke(job_id).query_async(conn).await;

        if let Err(err) = r {
            tracing::error!(
                "error when finish_job: {} - {}, {:?}",
                &self.queue.queue_name,
                job_id,
                err
            );
        }
    }

    pub(crate) fn invoke(&self, job_id: i64, conn: &mut dyn redis::ConnectionLike) {
        let r: redis::RedisResult<i64> = self.prepare_invoke(job_id).query(conn);

        if let Err(err) = r {
            tracing::error!(
                "error when finish_job: {} - {}, {:?}",
                &self.queue.queue_name,
                job_id,
                err
            );
        }
    }

    fn prepare_invoke(&self, job_id: i64) -> redis::Cmd {
        redis::Cmd::sadd(self.queue.done_key.as_str(), job_id)
    }
}

pub(crate) struct SleepOnJob {
    queue: Queue,
}

impl SleepOnJob {
    pub(crate) fn new(queue: Queue) -> Self {
        Self { queue }
    }

    pub(crate) async fn invoke_async<C: redis::aio::ConnectionLike>(
        &self,
        dequeue_sleep: DequeueSleep,
        conn: &mut C,
    ) {
        let r: redis::RedisResult<Option<String>> =
            self.prepare_invoke(dequeue_sleep).query_async(conn).await;

        match r {
            Ok(v) => {
                tracing::trace!("ok when sleep_on: {:?}", v);
            }
            Err(err) => {
                tracing::error!("error when sleep_on: {:?}", err);
            }
        }
    }

    pub(crate) fn invoke(&self, dequeue_sleep: DequeueSleep, conn: &mut dyn redis::ConnectionLike) {
        let r: redis::RedisResult<Option<String>> = self.prepare_invoke(dequeue_sleep).query(conn);

        match r {
            Ok(v) => {
                tracing::trace!("ok when sleep_on: {:?}", v);
            }
            Err(err) => {
                tracing::error!("error when sleep_on: {:?}", err);
            }
        }
    }

    fn prepare_invoke(&self, dequeue_sleep: DequeueSleep) -> redis::Cmd {
        let sleep_time = if dequeue_sleep.ndry_runs > 6 {
            18
        } else if dequeue_sleep.ndry_runs <= 0 {
            1
        } else {
            //                        dequeue_sleep.ndry_runs
            dequeue_sleep.ndry_runs * 3
        };

        let (src_key, dst_key) = match dequeue_sleep.sleep_on {
            crate::queue::SleepOn::SleepOnA => (
                self.queue.isleep_a_key.as_str(),
                self.queue.isleep_b_key.as_str(),
            ),
            crate::queue::SleepOn::SleepOnB => (
                self.queue.isleep_b_key.as_str(),
                self.queue.isleep_a_key.as_str(),
            ),
        };

        tracing::trace!("sleep_on - {} secs", sleep_time);

        redis::Cmd::brpoplpush(src_key, dst_key, sleep_time as usize)
    }
}
