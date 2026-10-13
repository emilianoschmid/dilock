use dilock::{Error, Options, ReleaseOutcome};
use dilock_etcd::EtcdProvider;
use std::{env, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = required("DILOCK_ETCD_ENDPOINT")?;
    let namespace = required("DILOCK_NAMESPACE")?;
    let epoch = required("DILOCK_EPOCH")?;
    let resource = format!("docs-example/rust-etcd/{}", std::process::id());
    let owner_provider = EtcdProvider::new(endpoint.clone(), namespace.clone(), epoch.clone())?;
    let contender_provider = EtcdProvider::new(endpoint, namespace, epoch.clone())?;
    let owner_lock = owner_provider.lock(&resource)?;
    let contender_lock = contender_provider.lock(&resource)?;
    let options = Options {
        lease: Duration::from_secs(5),
        rpc_timeout: Duration::from_millis(250),
        cleanup_timeout: Duration::from_secs(1),
        auto_renew: true,
        require_fencing: true,
    };

    let mut owner = owner_lock.acquire(Duration::from_secs(3), options).await?;
    let first = owner.fence().clone();
    println!(
        "owner: acquired with epoch-scoped create_revision {}/{}",
        first.epoch, first.revision
    );
    if !matches!(
        contender_lock
            .acquire(Duration::from_millis(300), options)
            .await,
        Err(Error::WaitTimeout)
    ) {
        let _ = owner.release().await;
        return Err("contender did not time out under acknowledged contention".into());
    }
    println!("contender: bounded queue wait timed out under contention");
    if owner.release().await != ReleaseOutcome::Released {
        return Err("owner release was not acknowledged".into());
    }

    let mut successor = contender_lock
        .acquire(Duration::from_secs(3), options)
        .await?;
    let next = successor.fence().clone();
    if next.epoch != epoch || next.revision <= first.revision {
        let _ = successor.release().await;
        return Err("successor fencing token did not advance within the configured epoch".into());
    }
    println!(
        "contender: acquired after release with create_revision {}",
        next.revision
    );
    if successor.release().await != ReleaseOutcome::Released {
        return Err("successor release was not acknowledged".into());
    }
    owner_provider.wait_for_cleanup().await;
    contender_provider.wait_for_cleanup().await;
    Ok(())
}

fn required(name: &str) -> Result<String, std::io::Error> {
    env::var(name).map_err(|_| std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!("{name} must name a pre-provisioned etcd namespace; this consumer does not create metadata"),
    ))
}
