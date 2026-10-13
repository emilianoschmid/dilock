use dilock::{Error, Options};
use dilock_etcd::{EtcdProvider, MetadataProvision};
use std::{env, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let endpoint =
        env::var("DILOCK_ETCD_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned());
    let epoch = env::var("DILOCK_EPOCH").unwrap_or_else(|_| "dev-1".to_owned());
    let arguments: Vec<_> = env::args().collect();
    if arguments.iter().any(|argument| argument == "--provision") {
        let provider = EtcdProvider::new(&endpoint, "default", &epoch)?;
        match provider.provision_namespace(Duration::from_secs(2)).await? {
            MetadataProvision::Created => println!("Created default namespace metadata."),
            MetadataProvision::AlreadyCompatible => {
                println!("Default namespace metadata already matches this epoch.")
            }
        }
    } else if arguments.iter().any(|argument| argument == "--configured") {
        configured(&endpoint, &epoch).await?;
    } else if arguments
        .iter()
        .any(|argument| argument == "--shared-provider")
    {
        shared_provider(&endpoint, &epoch).await?;
    } else {
        minimal(&endpoint, &epoch).await?;
    }
    Ok(())
}

async fn minimal(
    endpoint: &str,
    epoch: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let provider = EtcdProvider::new(endpoint, "default", epoch)?;
    let daily = provider.lock("reports/daily")?;
    let outcome = daily
        .with_lease(async |lease| {
            lease.ensure_held()?;
            println!(
                "Ran one short etcd protected operation (fence revision {}).",
                lease.fence().revision
            );
            Ok::<_, Error>(())
        })
        .await?;
    println!("Release observation: {:?}", outcome.release);
    Ok(())
}

async fn configured(
    endpoint: &str,
    epoch: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let provider = EtcdProvider::new(endpoint, "default", epoch)?;
    let daily = provider.lock("reports/daily-configured")?;
    let policy = Options {
        lease: Duration::from_secs(60),
        rpc_timeout: Duration::from_secs(2),
        cleanup_timeout: Duration::from_secs(2),
        auto_renew: false,
        require_fencing: false,
    };
    let outcome = daily
        .with_lease_with(Duration::from_secs(10), policy, async |lease| {
            lease.ensure_held()?;
            println!(
                "Ran one configured etcd operation (fence revision {}).",
                lease.fence().revision
            );
            Ok::<_, Error>(())
        })
        .await?;
    println!("Release observation: {:?}", outcome.release);
    Ok(())
}

async fn shared_provider(
    endpoint: &str,
    epoch: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let provider = EtcdProvider::new(endpoint, "default", epoch)?;
    let daily = provider.lock("reports/daily")?;
    let weekly = provider.lock("reports/weekly")?;
    let (daily, weekly) = tokio::try_join!(
        daily.with_lease(async |lease| {
            lease.ensure_held()?;
            println!(
                "Daily resource held via shared provider (revision {}).",
                lease.fence().revision
            );
            Ok::<_, Error>(())
        }),
        weekly.with_lease(async |lease| {
            lease.ensure_held()?;
            println!(
                "Weekly resource held via shared provider (revision {}).",
                lease.fence().revision
            );
            Ok::<_, Error>(())
        })
    )?;
    println!(
        "Two independent resources; release observations: {:?}, {:?}",
        daily.release, weekly.release
    );
    Ok(())
}
