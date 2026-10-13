use dilock::{Error, LeaseState, Options, ReleaseOutcome, WithLeaseError, redis_key};
use dilock_redis::RedisProvider;
use std::{
    env,
    io::{Read, Write},
    net::{SocketAddr, TcpStream as StdTcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Notify},
    task::JoinHandle,
};

static LIVE_TEST_LOCK: Mutex<()> = Mutex::const_new(());

/// Runs against a real, disposable Redis instance when explicitly enabled.
#[tokio::test]
#[ignore = "set DILOCK_REDIS_URL and rerun with --ignored to exercise real Redis"]
async fn acquisition_expiry_renewal_and_stale_release() {
    let _serial = LIVE_TEST_LOCK.lock().await;
    let endpoint = env::var("DILOCK_REDIS_URL")
        .expect("DILOCK_REDIS_URL is required for this real-server test");
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let namespace = format!("rust-test-{}-{unique:x}", std::process::id());
    let epoch = format!("test-{}", std::process::id());
    let client = redis::Client::open(endpoint.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let metadata = serde_json::json!({
        "protocol_major": 1,
        "identity_scheme": "sha256-framed-utf8-v1",
        "profile": "redis-single-authority-v1",
        "coordination_epoch": epoch.clone(),
        "fixed_lease_ms": null
    });
    let metadata_key = format!("dilock:namespace:{namespace}:meta");
    let created: Option<String> = redis::cmd("SET")
        .arg(&metadata_key)
        .arg(metadata.to_string())
        .arg("NX")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(created.as_deref(), Some("OK"));

    let provider = RedisProvider::new(endpoint.as_str(), namespace.clone(), epoch).unwrap();
    let short_wait_handle = provider.lock("short-wait-below-margin").unwrap();
    let short_wait_options = Options {
        rpc_timeout: Duration::from_millis(250),
        ..Options::default()
    };
    let mut short_wait_lease = short_wait_handle
        .acquire(Duration::from_millis(250), short_wait_options)
        .await
        .expect("an uncontended acquisition may complete below the lease safety margin");
    assert_eq!(short_wait_lease.state(), LeaseState::Held);
    assert_eq!(short_wait_lease.release().await, ReleaseOutcome::Released);

    let daily = provider.lock("reports/daily").unwrap();
    let weekly = provider.lock("reports/weekly").unwrap();
    let (daily, weekly) = tokio::try_join!(
        daily.with_lease(async |lease| {
            lease.ensure_held()?;
            Ok::<_, Error>("daily")
        }),
        weekly.with_lease(async |lease| {
            lease.ensure_held()?;
            Ok::<_, Error>("weekly")
        })
    )
    .expect("two distinct locks share one provider and release at scope exit");
    assert_eq!(daily.value, "daily");
    assert_eq!(weekly.value, "weekly");
    assert_eq!(daily.release, ReleaseOutcome::Released);
    assert_eq!(weekly.release, ReleaseOutcome::Released);
    let callback_error = provider
        .lock("reports/callback-error")
        .unwrap()
        .with_lease(async |_lease| Err::<(), _>("original work error"))
        .await;
    assert!(
        matches!(
            callback_error,
            Err(WithLeaseError::Work {
                source: "original work error",
                release: ReleaseOutcome::Released,
            })
        ),
        "unexpected callback/release result: {callback_error:?}"
    );

    let handle = provider.lock("same-resource").unwrap();
    let options = Options {
        lease: Duration::from_millis(150),
        rpc_timeout: Duration::from_millis(50),
        cleanup_timeout: Duration::from_millis(250),
        ..Options::default()
    };
    let mut old = handle.try_acquire(options).await.unwrap().unwrap();
    assert!(handle.try_acquire(options).await.unwrap().is_none());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(old.state(), LeaseState::Lost);

    let successor_options = Options {
        lease: Duration::from_millis(800),
        rpc_timeout: Duration::from_millis(100),
        cleanup_timeout: Duration::from_millis(250),
        ..Options::default()
    };
    let mut successor = handle
        .try_acquire(successor_options)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.release().await, ReleaseOutcome::AlreadyNotOwned);
    assert!(
        handle
            .try_acquire(successor_options)
            .await
            .unwrap()
            .is_none()
    );
    assert!(successor.renew().await.unwrap());
    assert_eq!(successor.release().await, ReleaseOutcome::Released);

    let wrong_type_resource = "wrong-type-resource";
    let wrong_type_key = redis_key(&namespace, wrong_type_resource).unwrap();
    let _: i64 = redis::cmd("RPUSH")
        .arg(&wrong_type_key)
        .arg("preserve-me")
        .query_async(&mut connection)
        .await
        .unwrap();
    let wrong_type_handle = provider.lock(wrong_type_resource).unwrap();
    assert!(matches!(
        wrong_type_handle.try_acquire(successor_options).await,
        Err(Error::ProtocolMismatch)
    ));
    let values: Vec<String> = redis::cmd("LRANGE")
        .arg(&wrong_type_key)
        .arg(0)
        .arg(-1)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(values, ["preserve-me"]);

    // Dropping an ordinary held lease schedules the same owner-checked cleanup
    // used by cancellation. Verify the fresh cleanup transport also completes
    // without an in-flight command or an explicit release call.
    let dropped_resource = "drop-cleanup-independent-connection";
    let dropped_key = redis_key(&namespace, dropped_resource).unwrap();
    let dropped_lease = provider
        .lock(dropped_resource)
        .unwrap()
        .try_acquire(successor_options)
        .await
        .unwrap()
        .expect("drop cleanup resource should be available");
    let present: Option<String> = redis::cmd("GET")
        .arg(&dropped_key)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(present.is_some(), "lease key should exist before Drop");
    drop(dropped_lease);
    provider.wait_for_cleanup().await;
    let after_drop: Option<String> = redis::cmd("GET")
        .arg(&dropped_key)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(
        after_drop.is_none(),
        "Drop cleanup should remove the lease key"
    );
    let _: i64 = redis::cmd("DEL")
        .arg(&wrong_type_key)
        .arg(&metadata_key)
        .query_async(&mut connection)
        .await
        .unwrap();
}

/// Cancels acquisition only after the shared RESP proxy confirms Redis applied
/// SET NX PX and is withholding its successful acknowledgement. The future must
/// not return an unobserved lease, and its dropped cleanup guard must remove
/// only that attempt's owner token.
#[tokio::test]
#[ignore = "set the Redis applied-SET proxy environment and rerun with --ignored"]
async fn cancelled_acquisition_after_applied_set_is_cleaned_by_owner() {
    let _serial = LIVE_TEST_LOCK.lock().await;
    let proxy_endpoint = env::var("DILOCK_REDIS_URL")
        .expect("DILOCK_REDIS_URL must point at the shared applied-SET proxy");
    let verify_endpoint = env::var("DILOCK_REDIS_VERIFY_URL")
        .expect("DILOCK_REDIS_VERIFY_URL must point directly at disposable Redis");
    let control_endpoint = env::var("DILOCK_REDIS_PROXY_CONTROL")
        .expect("DILOCK_REDIS_PROXY_CONTROL must point at proxy control port");
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let namespace = format!("rust-cancel-applied-set-{}-{unique:x}", std::process::id());
    let epoch = format!("test-{}", std::process::id());
    let resource = "cancel-after-applied-set";
    let metadata_key = format!("dilock:namespace:{namespace}:meta");
    let admin_client = redis::Client::open(verify_endpoint.as_str()).unwrap();
    let mut admin = admin_client
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let metadata = serde_json::json!({
        "protocol_major": 1,
        "identity_scheme": "sha256-framed-utf8-v1",
        "profile": "redis-single-authority-v1",
        "coordination_epoch": epoch.clone(),
        "fixed_lease_ms": null
    });
    let created: Option<String> = redis::cmd("SET")
        .arg(&metadata_key)
        .arg(metadata.to_string())
        .arg("NX")
        .query_async(&mut admin)
        .await
        .unwrap();
    assert_eq!(created.as_deref(), Some("OK"));

    let initial_stats = shared_redis_proxy_stats(&control_endpoint);
    assert_eq!(initial_stats["delayed_set_count"].as_u64(), Some(0));
    assert_eq!(initial_stats["eval_count"].as_u64(), Some(0));
    assert_eq!(initial_stats["dropped_count"].as_u64(), Some(0));

    let provider = RedisProvider::new(proxy_endpoint.as_str(), namespace.clone(), epoch).unwrap();
    let handle = provider.lock(resource).unwrap();
    let key = redis_key(&namespace, resource).unwrap();
    let acquiring = tokio::spawn(async move {
        handle
            .try_acquire(Options {
                lease: Duration::from_secs(30),
                // The shared proxy delays SET's ACK by two seconds; leave
                // enough independent cleanup budget for its ordered response.
                cleanup_timeout: Duration::from_secs(5),
                // Cancel inside the ACK delay, before the RPC timeout path can
                // start its own cleanup attempt.
                rpc_timeout: Duration::from_secs(10),
                ..Options::default()
            })
            .await
    });

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let stats = shared_redis_proxy_stats(&control_endpoint);
            if stats["delayed_set_count"].as_u64() == Some(1) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("proxy observed applied SET NX PX and is holding its +OK");

    let value: Option<String> = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut admin)
        .await
        .unwrap();
    assert!(value.is_some(), "Redis applied the pending acquisition");

    acquiring.abort();
    assert!(matches!(acquiring.await, Err(error) if error.is_cancelled()));
    provider.wait_for_cleanup().await;

    let stats = shared_redis_proxy_stats(&control_endpoint);
    eprintln!("proxy counters after cancellation cleanup: {stats}");
    let value: Option<String> = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut admin)
        .await
        .unwrap();
    assert_eq!(value, None, "owner-checked cleanup removed the late SET");
    assert_eq!(stats["delayed_set_count"].as_u64(), Some(1));
    assert_eq!(stats["eval_count"].as_u64(), Some(1));
    assert_eq!(stats["dropped_count"].as_u64(), Some(0));

    // Let the proxy forward the original acknowledgement after cancellation;
    // it must not cause a late lease to be returned or another cleanup RPC.
    tokio::time::sleep(Duration::from_millis(2100)).await;
    assert_eq!(
        shared_redis_proxy_stats(&control_endpoint)["eval_count"].as_u64(),
        Some(1)
    );
    let _: i64 = redis::cmd("DEL")
        .arg(&metadata_key)
        .query_async(&mut admin)
        .await
        .unwrap();
    provider.wait_for_cleanup().await;
    eprintln!(
        "PASS: applied SET NX PX was cancelled before +OK; no lease escaped, one owner EVAL removed the key, and no retry occurred"
    );
}

fn shared_redis_proxy_stats(endpoint: &str) -> serde_json::Value {
    let mut stream =
        StdTcpStream::connect(endpoint).expect("connect to shared Redis proxy control");
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    stream.write_all(b"STATS\n").unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    serde_json::from_str(response.trim()).expect("parse shared Redis proxy stats")
}

/// Cancels explicit release after Redis has received the command, while the
/// server is paused. The multiplexed connection may still apply the command;
/// the lease must preserve UNKNOWN and never submit another script on retry.
#[tokio::test]
#[ignore = "set DILOCK_REDIS_URL and rerun with --ignored to exercise real Redis"]
async fn cancelled_release_is_terminal_after_server_applies_it() {
    let _serial = LIVE_TEST_LOCK.lock().await;
    let endpoint = env::var("DILOCK_REDIS_URL")
        .expect("DILOCK_REDIS_URL is required for this real-server test");
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let namespace = format!("rust-release-cancel-{}-{unique:x}", std::process::id());
    let epoch = format!("test-{}", std::process::id());
    let client = redis::Client::open(endpoint.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let metadata_key = format!("dilock:namespace:{namespace}:meta");
    let metadata = serde_json::json!({
        "protocol_major": 1,
        "identity_scheme": "sha256-framed-utf8-v1",
        "profile": "redis-single-authority-v1",
        "coordination_epoch": epoch.clone(),
        "fixed_lease_ms": null
    });
    let created: Option<String> = redis::cmd("SET")
        .arg(&metadata_key)
        .arg(metadata.to_string())
        .arg("NX")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(created.as_deref(), Some("OK"));

    let proxy = RedisAckProxy::start("redis:6379").await;
    let proxy_endpoint = format!("redis://{}/", proxy.address);

    let provider =
        dilock_redis::RedisProvider::new(proxy_endpoint.as_str(), namespace.clone(), epoch)
            .unwrap();
    let handle = provider.lock("release-cancellation/live").unwrap();
    let lease = handle
        .try_acquire(Options {
            lease: Duration::from_secs(5),
            ..Options::default()
        })
        .await
        .unwrap()
        .unwrap();
    let key = redis_key(&namespace, "release-cancellation/live").unwrap();
    let before = redis_script_call_count(&mut connection).await;
    let shared_lease = Arc::new(Mutex::new(lease));

    proxy.arm();
    let releasing = shared_lease.clone();
    let release_task = tokio::spawn(async move { releasing.lock().await.release().await });
    tokio::time::timeout(Duration::from_secs(5), proxy.applied.notified())
        .await
        .expect("proxy observed the real EVAL reply after Redis applied release");
    let key_exists: i64 = redis::cmd("EXISTS")
        .arg(&key)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(key_exists, 0, "Redis applied the owner-checked release");
    release_task.abort();
    assert!(release_task.await.unwrap_err().is_cancelled());

    let applied_count = redis_script_call_count(&mut connection).await;
    assert!(applied_count > before);
    {
        let mut lease = shared_lease.lock().await;
        assert_eq!(lease.state(), LeaseState::Releasing);
        assert_eq!(lease.release().await, ReleaseOutcome::Unknown);
    }
    let after_retry = redis_script_call_count(&mut connection).await;
    assert_eq!(
        after_retry, applied_count,
        "repeated release must not retry EVALSHA"
    );
    proxy.release_ack.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(redis_script_call_count(&mut connection).await, after_retry);

    let mut successor = handle
        .try_acquire(Options::default())
        .await
        .unwrap()
        .expect("a successor acquires after the cancelled command applied");
    assert_eq!(successor.release().await, ReleaseOutcome::Released);
    assert_eq!(
        redis_script_call_count(&mut connection).await,
        after_retry + 1
    );

    provider.wait_for_cleanup().await;
    proxy.task.abort();
    let _: i64 = redis::cmd("DEL")
        .arg(&metadata_key)
        .query_async(&mut connection)
        .await
        .unwrap();
    let _: String = redis::cmd("QUIT")
        .query_async(&mut connection)
        .await
        .unwrap_or_default();
}

/// Forwards Redis TCP traffic and withholds only the reply to the first EVAL
/// observed after arming. This creates an applied-command/lost-ack race without
/// changing Redis or relying on a timing pause in the server.
struct RedisAckProxy {
    address: SocketAddr,
    armed: Arc<AtomicBool>,
    applied: Arc<Notify>,
    release_ack: Arc<Notify>,
    task: JoinHandle<()>,
}

impl RedisAckProxy {
    async fn start(target: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let armed = Arc::new(AtomicBool::new(false));
        let hold_next_reply = Arc::new(AtomicBool::new(false));
        let applied = Arc::new(Notify::new());
        let release_ack = Arc::new(Notify::new());
        let task_armed = armed.clone();
        let task_applied = applied.clone();
        let task_release_ack = release_ack.clone();
        let target = target.to_owned();
        let task = tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else {
                    return;
                };
                let Ok(server) = TcpStream::connect(&target).await else {
                    continue;
                };
                let armed = task_armed.clone();
                let hold_next_reply = hold_next_reply.clone();
                let applied = task_applied.clone();
                let release_ack = task_release_ack.clone();
                tokio::spawn(async move {
                    let (mut client_read, mut client_write) = client.into_split();
                    let (mut server_read, mut server_write) = server.into_split();
                    let client_to_server = async {
                        let mut buffer = [0_u8; 8192];
                        let mut observed = Vec::new();
                        loop {
                            let count = client_read.read(&mut buffer).await?;
                            if count == 0 {
                                return Ok::<(), std::io::Error>(());
                            }
                            let bytes = &buffer[..count];
                            observed.extend_from_slice(bytes);
                            if observed
                                .windows(b"$4\r\nEVAL\r\n".len())
                                .any(|window| window == b"$4\r\nEVAL\r\n")
                                && armed.load(Ordering::SeqCst)
                            {
                                armed.store(false, Ordering::SeqCst);
                                hold_next_reply.store(true, Ordering::SeqCst);
                            }
                            if observed.len() > 64 {
                                observed.drain(..observed.len() - 64);
                            }
                            server_write.write_all(bytes).await?;
                        }
                    };
                    let server_to_client = async {
                        let mut buffer = [0_u8; 8192];
                        loop {
                            let count = server_read.read(&mut buffer).await?;
                            if count == 0 {
                                return Ok::<(), std::io::Error>(());
                            }
                            if hold_next_reply.swap(false, Ordering::SeqCst) {
                                applied.notify_one();
                                release_ack.notified().await;
                            }
                            client_write.write_all(&buffer[..count]).await?;
                        }
                    };
                    let _ = tokio::try_join!(client_to_server, server_to_client);
                });
            }
        });
        Self {
            address,
            armed,
            applied,
            release_ack,
            task,
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

async fn redis_script_call_count(connection: &mut redis::aio::MultiplexedConnection) -> u64 {
    let info: String = redis::cmd("INFO")
        .arg("commandstats")
        .query_async(connection)
        .await
        .unwrap();
    info.lines()
        .filter_map(|line| {
            let (name, stats) = line.split_once(':')?;
            if name != "cmdstat_eval" && name != "cmdstat_evalsha" {
                return None;
            }
            stats
                .split(',')
                .find_map(|field| field.strip_prefix("calls="))
                .and_then(|calls| calls.parse::<u64>().ok())
        })
        .sum()
}
