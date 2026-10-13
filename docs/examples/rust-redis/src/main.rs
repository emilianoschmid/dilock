use dilock::{Error, Options, ReleaseOutcome};
use dilock_redis::RedisProvider;
use std::{env, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = required_env("DILOCK_REDIS_URL")?;
    let namespace = required_env("DILOCK_NAMESPACE")?;
    let epoch = required_env("DILOCK_EPOCH")?;
    let resource_name =
        env::var("DILOCK_RESOURCE").unwrap_or_else(|_| "docs/rust-redis-consumer".to_owned());

    // Each provider owns a separate Redis client connection. Both select the
    // same operator-provisioned authority and namespace.
    let first_provider = RedisProvider::new(&endpoint, &namespace, &epoch)?;
    let second_provider = RedisProvider::new(&endpoint, &namespace, &epoch)?;
    let first = first_provider.lock(&resource_name)?;
    let second = second_provider.lock(&resource_name)?;

    let short = Options {
        lease: Duration::from_millis(1_200),
        rpc_timeout: Duration::from_millis(200),
        cleanup_timeout: Duration::from_millis(250),
        ..Options::default()
    };
    let mut expired_owner = first
        .try_acquire(short)
        .await?
        .ok_or("resource was busy before the example started")?;
    expired_owner.ensure_held()?;

    match second.try_acquire(short).await {
        Ok(None) => println!("contender: acknowledged busy"),
        Ok(Some(mut unexpected)) => {
            let _ = unexpected.release().await;
            return Err("contender unexpectedly acquired while the first lease was live".into());
        }
        Err(error) => return Err(error.into()),
    }

    match second.acquire(Duration::from_millis(180), short).await {
        Err(Error::WaitTimeout) => println!("contender: bounded wait ended with timeout"),
        Err(error) => return Err(error.into()),
        Ok(mut unexpected) => {
            let _ = unexpected.release().await;
            return Err("bounded wait unexpectedly acquired before owner expiry".into());
        }
    }

    // Expiry ends local validity; it does not stop work or prove Redis is
    // reachable. Wait past the requested TTL before testing a successor.
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    let mut successor = second
        .try_acquire(Options {
            lease: Duration::from_secs(3),
            ..Options::default()
        })
        .await?
        .ok_or("successor remained busy after the lease TTL")?;
    successor.ensure_held()?;
    println!("contender: acquired after Redis TTL");

    // A stale owner's delayed release must compare ownership and leave the
    // successor's key intact.
    match expired_owner.release().await {
        ReleaseOutcome::AlreadyNotOwned => {
            println!("original owner: stale release left successor untouched")
        }
        other => {
            return Err(format!("stale release returned unexpected outcome: {other:?}").into());
        }
    }
    match second.try_acquire(Options::default()).await {
        Ok(None) => println!("successor: still owns the resource"),
        Ok(Some(mut unexpected)) => {
            let _ = unexpected.release().await;
            return Err("successor key disappeared after stale release".into());
        }
        Err(error) => return Err(error.into()),
    }
    if successor.release().await != ReleaseOutcome::Released {
        return Err("successor release was not acknowledged".into());
    }
    first_provider.wait_for_cleanup().await;
    second_provider.wait_for_cleanup().await;
    println!("successor: release acknowledged");
    Ok(())
}

fn required_env(name: &str) -> Result<String, std::io::Error> {
    env::var(name).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{name} must name an existing Redis setup; this example does not provision it"),
        )
    })
}
