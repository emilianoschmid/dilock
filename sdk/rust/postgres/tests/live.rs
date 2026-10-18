use dilock::{Error, Options, ReleaseOutcome};
use dilock_postgres::{PostgresOptions, PostgresProvider};
use std::env;
use std::time::Duration;

#[tokio::test]
async fn postgres_live_contention_and_handoff() {
    let Some(dsn) = env::var_os("DILOCK_POSTGRES_DSN") else {
        return;
    };
    let options = PostgresOptions {
        connection_string: dsn.to_string_lossy().into_owned(),
        namespace: "ci-postgres-rust".into(),
        coordination_epoch: "ci-postgres-rust-epoch".into(),
        rpc_timeout: Duration::from_millis(500),
        cleanup_timeout: Duration::from_millis(500),
        poll_interval: Duration::from_millis(10),
        default_wait: Duration::from_millis(250),
        default_options: Options {
            lease: Duration::from_secs(5),
            rpc_timeout: Duration::from_millis(500),
            cleanup_timeout: Duration::from_millis(500),
            ..Options::default()
        },
    };
    PostgresProvider::provision_namespace(
        &options.connection_string,
        &options.namespace,
        &options.coordination_epoch,
    )
    .await
    .unwrap();
    let first = PostgresProvider::connect(options.clone()).await.unwrap();
    let second = PostgresProvider::connect(options.clone()).await.unwrap();
    let first_lock = first.lock("rust-provider").unwrap();
    let second_lock = second.lock("rust-provider").unwrap();
    let mut lease = first_lock
        .try_acquire(options.default_options)
        .await
        .unwrap()
        .unwrap();
    assert!(lease.renew().await.unwrap());
    assert!(second_lock
        .try_acquire(options.default_options)
        .await
        .unwrap()
        .is_none());
    let different = second.lock("rust-different-resource").unwrap();
    let mut different_lease = different
        .try_acquire(options.default_options)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(different_lease.release().await.unwrap(), ReleaseOutcome::Released);
    assert!(matches!(
        second_lock
            .acquire(Duration::from_millis(150), options.default_options)
            .await,
        Err(Error::WaitTimeout)
    ));
    assert!(matches!(
        second_lock
            .try_acquire(Options {
                require_fencing: true,
                ..options.default_options
            })
            .await,
        Err(Error::UnsupportedCapability)
    ));
    assert_eq!(lease.release().await.unwrap(), ReleaseOutcome::Released);
    let mut handoff = second_lock
        .try_acquire(options.default_options)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(handoff.release().await.unwrap(), ReleaseOutcome::Released);
    first.close();
    second.close();
}
