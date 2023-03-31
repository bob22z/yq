use redis::{aio::ConnectionLike, Client};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::open("redis://127.0.0.1/")?;
    let mut redis_conn = client.get_tokio_connection_manager().await?;

    let keys: Vec<String> = redis::Cmd::keys("yq:*")
        .query_async(&mut redis_conn)
        .await?;
    if keys.is_empty() {
        println!("NO KEYS");
    } else {
        for key in keys {
            print_key_value(&key, &mut redis_conn).await?;
        }
    }

    Ok(())
}

async fn print_key_value<C>(key: &str, conn: &mut C) -> Result<(), Box<dyn std::error::Error>>
where
    C: ConnectionLike,
{
    let key_type: String = redis::cmd("TYPE").arg(key).query_async(conn).await?;
    println!("key: {key} - {key_type}");
    match key_type.as_str() {
        "string" => print_string_value(key, conn).await?,
        "list" => print_list_value(key, conn).await?,
        "hash" => print_hash_value(key, conn).await?,
        "zset" => print_zset_value(key, conn).await?,
        _ => {
            println!("\t ERROR: unknown key_type: {key_type}");
        }
    }
    Ok(())
}

async fn print_string_value<C>(key: &str, conn: &mut C) -> Result<(), Box<dyn std::error::Error>>
where
    C: ConnectionLike,
{
    let value: String = redis::Cmd::get(key).query_async(conn).await?;
    println!("\t{value}");
    Ok(())
}

async fn print_list_value<C>(key: &str, conn: &mut C) -> Result<(), Box<dyn std::error::Error>>
where
    C: ConnectionLike,
{
    let values: Vec<String> = redis::Cmd::lrange(key, 0, -1).query_async(conn).await?;
    if values.is_empty() {
        println!("\t EMPTY LIST");
    } else {
        for value in values {
            println!("\t{value}");
        }
    }
    Ok(())
}

async fn print_hash_value<C>(key: &str, conn: &mut C) -> Result<(), Box<dyn std::error::Error>>
where
    C: ConnectionLike,
{
    let values: HashMap<String, String> = redis::Cmd::hgetall(key).query_async(conn).await?;
    if values.is_empty() {
        println!("\t EMPTY HASH");
    } else {
        for (k, v) in values {
            println!("\t k={k}, v={v}");
        }
    }
    Ok(())
}

async fn print_zset_value<C>(key: &str, conn: &mut C) -> Result<(), Box<dyn std::error::Error>>
where
    C: ConnectionLike,
{
    let rows: Vec<(String, i64)> = redis::Cmd::zrange_withscores(key, 0, -1)
        .query_async(conn)
        .await?;
    if rows.is_empty() {
        println!("\t EMPTY ZSET");
    } else {
        for (value, score) in rows {
            println!("\t value={value}, score={score}");
        }
    }
    Ok(())
}
