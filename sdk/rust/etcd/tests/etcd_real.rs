use dilock::{Error, LeaseState, Options, ReleaseOutcome, WithLeaseError};
use dilock_etcd::{EtcdProvider, MetadataProvision};
use std::{
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn options() -> Options {
    Options {
        lease: Duration::from_secs(2),
        rpc_timeout: Duration::from_secs(1),
        cleanup_timeout: Duration::from_secs(2),
        ..Options::default()
    }
}

fn renewing_options() -> Options {
    Options {
        auto_renew: true,
        ..options()
    }
}

fn provider() -> (EtcdProvider, String, String) {
    let endpoint =
        env::var("DILOCK_ETCD_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:22379".to_owned());
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    let namespace = format!(
        "rust-test-{:x}",
        unique & 0x0000_ffff_ffff_ffff_ffff_ffff_ffff
    );
    let provider = EtcdProvider::new(
        endpoint.clone(),
        namespace.clone(),
        "local-etcd-3-7-2-test-epoch",
    )
    .expect("valid provider");
    (provider, namespace, endpoint)
}

#[tokio::test]
#[ignore = "requires a disposable etcd endpoint; set DILOCK_ETCD_ENDPOINT"]
async fn native_lock_keepalive_fifo_fence_and_cancellation_cleanup() {
    let (provider, namespace, endpoint) = provider();
    let unprovisioned = provider.lock("queue/progress").unwrap();
    assert!(matches!(
        unprovisioned
            .acquire(Duration::from_secs(1), options())
            .await,
        Err(Error::ProtocolMismatch)
    ));
    assert_eq!(
        provider
            .provision_namespace(Duration::from_secs(2))
            .await
            .unwrap(),
        MetadataProvision::Created
    );
    assert_eq!(
        provider
            .provision_namespace(Duration::from_secs(2))
            .await
            .unwrap(),
        MetadataProvision::AlreadyCompatible
    );

    let daily = provider.lock("reports/daily").unwrap();
    let weekly = provider.lock("reports/weekly").unwrap();
    let (daily, weekly) = tokio::try_join!(
        daily.with_lease(async |lease| {
            lease.ensure_held()?;
            Ok::<_, Error>(lease.fence().revision)
        }),
        weekly.with_lease(async |lease| {
            lease.ensure_held()?;
            Ok::<_, Error>(lease.fence().revision)
        })
    )
    .expect("two distinct locks share one provider and release at scope exit");
    assert!(daily.value > 0);
    assert!(weekly.value > 0);
    assert_ne!(daily.value, weekly.value);
    assert_eq!(daily.release, ReleaseOutcome::Released);
    assert_eq!(weekly.release, ReleaseOutcome::Released);
    let callback_error = provider
        .lock("reports/callback-error")
        .unwrap()
        .with_lease(async |_lease| Err::<(), _>("original work error"))
        .await;
    assert!(matches!(
        callback_error,
        Err(WithLeaseError::Work {
            source: "original work error",
            release: ReleaseOutcome::Released,
        })
    ));

    let incompatible_epoch = EtcdProvider::new(endpoint, namespace, "wrong-epoch").unwrap();
    assert!(matches!(
        incompatible_epoch
            .provision_namespace(Duration::from_secs(2))
            .await,
        Err(Error::ProtocolMismatch)
    ));

    let handle = provider.lock("queue/progress").unwrap();
    let first = handle
        .acquire(Duration::from_secs(3), renewing_options())
        .await
        .expect("first native Lock succeeds");
    assert_eq!(first.fence().epoch, "local-etcd-3-7-2-test-epoch");
    let first_revision = first.fence().revision;
    assert!(first_revision > 0);

    let waiting_handle = handle.clone();
    let waiter = tokio::spawn(async move {
        waiting_handle
            .acquire(Duration::from_secs(8), renewing_options())
            .await
    });
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(!waiter.is_finished(), "waiter should survive a lease TTL");

    let mut first = first;
    assert_eq!(first.state(), LeaseState::Held);
    assert_eq!(first.release().await, ReleaseOutcome::Released);
    assert_eq!(first.release().await, ReleaseOutcome::Released);
    let second = waiter
        .await
        .expect("waiter task survived")
        .expect("native queue progressed");
    assert!(second.fence().revision > first_revision);
    assert_eq!(second.fence().epoch, first.fence().epoch);

    let waiting_handle = handle.clone();
    let cancelled = tokio::spawn(async move {
        waiting_handle
            .acquire(Duration::from_secs(8), renewing_options())
            .await
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    cancelled.abort();
    let _ = cancelled.await;
    provider.wait_for_cleanup().await;

    let mut second = second;
    assert_eq!(second.release().await, ReleaseOutcome::Released);
    let third = handle
        .acquire(Duration::from_secs(5), options())
        .await
        .expect("lock succeeds after cancelled waiter was revoked");
    assert!(third.fence().revision > second.fence().revision);
    let mut third = third;
    assert_eq!(third.release().await, ReleaseOutcome::Released);

    assert!(matches!(
        EtcdProvider::new(
            env::var("DILOCK_ETCD_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:22379".to_owned()),
            "Bad Namespace",
            "epoch"
        ),
        Err(Error::InvalidIdentity(_))
    ));
}

#[tokio::test]
#[ignore = "requires a disposable etcd endpoint; set DILOCK_ETCD_ENDPOINT"]
async fn auto_renew_disabled_expires_and_stale_release_is_already_not_owned() {
    let (provider, _, _) = provider();
    provider
        .provision_namespace(Duration::from_secs(2))
        .await
        .expect("namespace metadata is provisioned");
    let handle = provider.lock("expiry/no-auto-renew").unwrap();
    let mut first = handle
        .acquire(Duration::from_secs(2), options())
        .await
        .expect("first lock acquired");
    let first_fence = first.fence().revision;
    tokio::time::sleep(Duration::from_millis(2300)).await;
    let successor = handle
        .acquire(Duration::from_secs(3), options())
        .await
        .expect("successor acquires after the non-renewed lease expires");
    assert!(successor.fence().revision > first_fence);
    assert_eq!(first.release().await, ReleaseOutcome::AlreadyNotOwned);
    let mut successor = successor;
    assert_eq!(successor.release().await, ReleaseOutcome::Released);
}

#[tokio::test]
#[ignore = "requires a disposable etcd endpoint; set DILOCK_ETCD_ENDPOINT"]
async fn applied_unlock_and_revoke_ack_loss_is_terminal_and_preserves_successor_fence() {
    let (provider, _, _) = provider();
    provider
        .provision_namespace(Duration::from_secs(2))
        .await
        .expect("namespace metadata is provisioned");
    let handle = provider.lock("fault/applied-cleanup-ack-loss").unwrap();
    let mut owner = handle
        .acquire(Duration::from_secs(3), options())
        .await
        .expect("first native lock acquired");
    let owner_revision = owner.fence().revision;

    // The test-only gRPC proxy consumes the successful etcd responses for both
    // Unlock and this lease's Revoke, then returns Unavailable for each. With
    // neither acknowledgement available, exact-key absence still cannot prove
    // the unique lease was revoked; the terminal outcome must remain unknown.
    let outcome = owner.release().await;
    assert_eq!(outcome, ReleaseOutcome::Unknown);
    assert_eq!(owner.state(), LeaseState::ReleasedUnknown);
    assert_eq!(owner.release().await, outcome, "terminal release is cached");

    let mut successor = handle
        .acquire(Duration::from_secs(3), options())
        .await
        .expect("successor acquires after both cleanup RPCs applied");
    assert!(successor.fence().revision > owner_revision);
    assert_eq!(successor.release().await, ReleaseOutcome::Released);
    provider.wait_for_cleanup().await;
}
