use dilock_consul::{ConsulOptions, ConsulProvider, ReleaseOutcome};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = ConsulOptions {
        endpoint: std::env::var("DILOCK_CONSUL_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:8500".into()),
        namespace: "billing".into(),
        coordination_epoch: "local-consul-epoch".into(),
        acl_token: std::env::var("DILOCK_CONSUL_ACL_TOKEN").unwrap_or_default(),
        ..ConsulOptions::default()
    };
    dilock_consul::provision_namespace(&options)?;
    let provider = ConsulProvider::new(&options)?;
    let handle = provider.lock("invoice/42/reconcile")?;
    let mut lease = handle.acquire_default()?;
    if !lease.is_valid() {
        return Err("lease is no longer valid".into());
    }
    let fence = lease.fence()?;
    println!("Consul fence: {}:{}", fence.coordination_epoch, fence.lock_index);
    if lease.release()? != ReleaseOutcome::Released {
        return Err("release was not confirmed".into());
    }
    Ok(())
}
