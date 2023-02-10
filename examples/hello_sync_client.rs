use serde::{Deserialize, Serialize};
use yq::sync::SyncClient;
use yq::{Job, JobType};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = SyncClient::new("redis://127.0.0.1/")?;

    let hello_job = HelloJob {
        id: 4,
        name: "Bob".into(),
    };
    client.schedule(&hello_job)?;

    let hello_job2 = HelloJob {
        id: 5,
        name: "Bob".into(),
    };
    client.schedule(&hello_job2)?;

    let hello_job3 = HelloJob {
        id: 6,
        name: "Bob".into(),
    };
    client.schedule(&hello_job3)?;

    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
struct HelloJob {
    id: i64,
    name: String,
}

impl Job for HelloJob {
    const JOB_TYPE: JobType = JobType::Borrowed("HelloJob");
    type State = ();
}
