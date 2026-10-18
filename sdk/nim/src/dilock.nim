## Experimental Nim facade over the repository C ABI. No Redis/etcd protocol
## or transport implementation is duplicated here.
import std/options

type
  DilockStatus* = enum
    dsOk = 0, dsBusy = 1, dsInvalidArgument = 2, dsCancelled = 3,
    dsWaitTimeout = 4, dsBackendUnavailable = 5, dsAcquisitionUnknown = 6,
    dsProtocolMismatch = 7, dsLeaseLost = 8, dsUnsupportedCapability = 9,
    dsInternal = 10

  DilockError* = object of CatchableError
    status*: DilockStatus

  ReleaseOutcome* = enum
    released = 0, alreadyNotOwned = 1, releaseUnknown = 2

  CBackend {.importc: "dilock_backend", header: "dilock/dilock.h", size: sizeof(cint).} = enum
    cBackendRedis = 0, cBackendEtcd = 1, cBackendConsul = 2, cBackendPostgres = 3
  CReleaseOutcome {.importc: "dilock_release_outcome", header: "dilock/dilock.h", size: sizeof(cint).} = enum
    cReleased = 0, cAlreadyNotOwned = 1, cReleaseUnknown = 2

  CleanupCallback = proc(backend: CBackend, outcome: CReleaseOutcome,
                         reason: cstring, userData: pointer) {.cdecl.}
  CError {.importc: "dilock_error", header: "dilock/dilock.h", bycopy.} = object
    code: int32
    message: array[384, char]

  CRedisOptions {.importc: "dilock_redis_options", header: "dilock/dilock.h", bycopy.} = object
    host: cstring
    port: int32
    username: cstring
    password: cstring
    database: int32
    name_space: cstring
    coordination_epoch: cstring
    cleanup_callback: CleanupCallback
    cleanup_user_data: pointer
    rpc_timeout_ms: int64
    cleanup_timeout_ms: int64
    safety_margin_ms: int64
    poll_interval_ms: int64
    default_wait_ms: int64
    default_lease_ms: int64

  CEtcdOptions {.importc: "dilock_etcd_options", header: "dilock/dilock.h", bycopy.} = object
    endpoint: cstring
    name_space: cstring
    coordination_epoch: cstring
    tls_root_certificates: cstring
    tls_client_certificate: cstring
    tls_client_private_key: cstring
    cleanup_callback: CleanupCallback
    cleanup_user_data: pointer
    rpc_timeout_ms: int64
    cleanup_timeout_ms: int64
    safety_margin_ms: int64
    default_wait_ms: int64
    default_lease_ms: int64
    auto_renew: int32

  CConsulOptions {.importc: "dilock_consul_options", header: "dilock/dilock.h", bycopy.} = object
    endpoint: cstring
    name_space: cstring
    coordination_epoch: cstring
    acl_token: cstring
    cleanup_callback: CleanupCallback
    cleanup_user_data: pointer
    rpc_timeout_ms: int64
    cleanup_timeout_ms: int64
    poll_interval_ms: int64
    default_wait_ms: int64
    default_lease_ms: int64

  CPostgresOptions {.importc: "dilock_postgres_options", header: "dilock/dilock.h", bycopy.} = object
    connection_string: cstring
    name_space: cstring
    coordination_epoch: cstring
    cleanup_callback: CleanupCallback
    cleanup_user_data: pointer
    rpc_timeout_ms: int64
    cleanup_timeout_ms: int64
    poll_interval_ms: int64
    default_wait_ms: int64
    default_lease_ms: int64

  CRedisProvider {.importc: "dilock_redis_provider", header: "dilock/dilock.h", incompleteStruct.} = object
  CRedisLock {.importc: "dilock_redis_lock", header: "dilock/dilock.h", incompleteStruct.} = object
  CRedisLease {.importc: "dilock_redis_lease", header: "dilock/dilock.h", incompleteStruct.} = object
  CEtcdProvider {.importc: "dilock_etcd_provider", header: "dilock/dilock.h", incompleteStruct.} = object
  CEtcdLock {.importc: "dilock_etcd_lock", header: "dilock/dilock.h", incompleteStruct.} = object
  CEtcdLease {.importc: "dilock_etcd_lease", header: "dilock/dilock.h", incompleteStruct.} = object
  CConsulProvider {.importc: "dilock_consul_provider", header: "dilock/dilock.h", incompleteStruct.} = object
  CConsulLock {.importc: "dilock_consul_lock", header: "dilock/dilock.h", incompleteStruct.} = object
  CConsulLease {.importc: "dilock_consul_lease", header: "dilock/dilock.h", incompleteStruct.} = object
  CPostgresProvider {.importc: "dilock_postgres_provider", header: "dilock/dilock.h", incompleteStruct.} = object
  CPostgresLock {.importc: "dilock_postgres_lock", header: "dilock/dilock.h", incompleteStruct.} = object
  CPostgresLease {.importc: "dilock_postgres_lease", header: "dilock/dilock.h", incompleteStruct.} = object
  CCancellation {.importc: "dilock_cancellation", header: "dilock/dilock.h", incompleteStruct.} = object
  CEtcdFence {.importc: "dilock_etcd_fence", header: "dilock/dilock.h", bycopy.} = object
    create_revision: int64
    coordination_epoch_bytes: csize_t
  CConsulFence {.importc: "dilock_consul_fence", header: "dilock/dilock.h", bycopy.} = object
    lock_index: uint64
    coordination_epoch_bytes: csize_t

  RedisProvider* = ref object
    raw: ptr CRedisProvider
  RedisLock* = ref object
    raw: ptr CRedisLock
  RedisLease* = ref object
    raw: ptr CRedisLease
  EtcdProvider* = ref object
    raw: ptr CEtcdProvider
  EtcdLock* = ref object
    raw: ptr CEtcdLock
  EtcdLease* = ref object
    raw: ptr CEtcdLease
  ConsulProvider* = ref object
    raw: ptr CConsulProvider
  ConsulLock* = ref object
    raw: ptr CConsulLock
  ConsulLease* = ref object
    raw: ptr CConsulLease
  PostgresProvider* = ref object
    raw: ptr CPostgresProvider
  PostgresLock* = ref object
    raw: ptr CPostgresLock
  PostgresLease* = ref object
    raw: ptr CPostgresLease
  Cancellation* = ref object
    raw: ptr CCancellation
  EtcdFence* = object
    epoch*: string
    createRevision*: int64
  ConsulFence* = object
    epoch*: string
    lockIndex*: uint64

proc abiVersion*(): uint32 {.importc: "dilock_c_abi_version", header: "dilock/dilock.h", cdecl.}
proc cRedisOptionsInit(options: ptr CRedisOptions) {.importc: "dilock_redis_options_init", header: "dilock/dilock.h", cdecl.}
proc cEtcdOptionsInit(options: ptr CEtcdOptions) {.importc: "dilock_etcd_options_init", header: "dilock/dilock.h", cdecl.}
proc cConsulOptionsInit(options: ptr CConsulOptions) {.importc: "dilock_consul_options_init", header: "dilock/dilock.h", cdecl.}
proc cPostgresOptionsInit(options: ptr CPostgresOptions) {.importc: "dilock_postgres_options_init", header: "dilock/dilock.h", cdecl.}
proc cRedisProviderCreate(options: ptr CRedisOptions, output: ptr ptr CRedisProvider, error: ptr CError): cint {.importc: "dilock_redis_provider_create", header: "dilock/dilock.h", cdecl.}
proc cEtcdProviderCreate(options: ptr CEtcdOptions, output: ptr ptr CEtcdProvider, error: ptr CError): cint {.importc: "dilock_etcd_provider_create", header: "dilock/dilock.h", cdecl.}
proc cConsulProviderCreate(options: ptr CConsulOptions, output: ptr ptr CConsulProvider, error: ptr CError): cint {.importc: "dilock_consul_provider_create", header: "dilock/dilock.h", cdecl.}
proc cPostgresProviderCreate(options: ptr CPostgresOptions, output: ptr ptr CPostgresProvider, error: ptr CError): cint {.importc: "dilock_postgres_provider_create", header: "dilock/dilock.h", cdecl.}
proc cRedisProviderDestroy(provider: ptr CRedisProvider) {.importc: "dilock_redis_provider_destroy", header: "dilock/dilock.h", cdecl.}
proc cEtcdProviderDestroy(provider: ptr CEtcdProvider) {.importc: "dilock_etcd_provider_destroy", header: "dilock/dilock.h", cdecl.}
proc cConsulProviderDestroy(provider: ptr CConsulProvider) {.importc: "dilock_consul_provider_destroy", header: "dilock/dilock.h", cdecl.}
proc cPostgresProviderDestroy(provider: ptr CPostgresProvider) {.importc: "dilock_postgres_provider_destroy", header: "dilock/dilock.h", cdecl.}
proc cRedisLockCreate(provider: ptr CRedisProvider, resource: cstring, output: ptr ptr CRedisLock, error: ptr CError): cint {.importc: "dilock_redis_lock_create", header: "dilock/dilock.h", cdecl.}
proc cEtcdLockCreate(provider: ptr CEtcdProvider, resource: cstring, output: ptr ptr CEtcdLock, error: ptr CError): cint {.importc: "dilock_etcd_lock_create", header: "dilock/dilock.h", cdecl.}
proc cConsulLockCreate(provider: ptr CConsulProvider, resource: cstring, output: ptr ptr CConsulLock, error: ptr CError): cint {.importc: "dilock_consul_lock_create", header: "dilock/dilock.h", cdecl.}
proc cPostgresLockCreate(provider: ptr CPostgresProvider, resource: cstring, output: ptr ptr CPostgresLock, error: ptr CError): cint {.importc: "dilock_postgres_lock_create", header: "dilock/dilock.h", cdecl.}
proc cRedisLockDestroy(lock: ptr CRedisLock) {.importc: "dilock_redis_lock_destroy", header: "dilock/dilock.h", cdecl.}
proc cEtcdLockDestroy(lock: ptr CEtcdLock) {.importc: "dilock_etcd_lock_destroy", header: "dilock/dilock.h", cdecl.}
proc cConsulLockDestroy(lock: ptr CConsulLock) {.importc: "dilock_consul_lock_destroy", header: "dilock/dilock.h", cdecl.}
proc cPostgresLockDestroy(lock: ptr CPostgresLock) {.importc: "dilock_postgres_lock_destroy", header: "dilock/dilock.h", cdecl.}
proc cRedisAcquireDefault(lock: ptr CRedisLock, output: ptr ptr CRedisLease, error: ptr CError): cint {.importc: "dilock_redis_acquire_default", header: "dilock/dilock.h", cdecl.}
proc cRedisAcquireDefaultCancel(lock: ptr CRedisLock, cancellation: ptr CCancellation, output: ptr ptr CRedisLease, error: ptr CError): cint {.importc: "dilock_redis_acquire_default_with_cancellation", header: "dilock/dilock.h", cdecl.}
proc cRedisAcquire(lock: ptr CRedisLock, waitMs, leaseMs: int64, output: ptr ptr CRedisLease, error: ptr CError): cint {.importc: "dilock_redis_acquire", header: "dilock/dilock.h", cdecl.}
proc cEtcdAcquireDefault(lock: ptr CEtcdLock, output: ptr ptr CEtcdLease, error: ptr CError): cint {.importc: "dilock_etcd_acquire_default", header: "dilock/dilock.h", cdecl.}
proc cEtcdAcquireDefaultCancel(lock: ptr CEtcdLock, cancellation: ptr CCancellation, output: ptr ptr CEtcdLease, error: ptr CError): cint {.importc: "dilock_etcd_acquire_default_with_cancellation", header: "dilock/dilock.h", cdecl.}
proc cEtcdAcquire(lock: ptr CEtcdLock, waitMs, leaseMs: int64, output: ptr ptr CEtcdLease, error: ptr CError): cint {.importc: "dilock_etcd_acquire", header: "dilock/dilock.h", cdecl.}
proc cConsulAcquireDefault(lock: ptr CConsulLock, output: ptr ptr CConsulLease, error: ptr CError): cint {.importc: "dilock_consul_acquire_default", header: "dilock/dilock.h", cdecl.}
proc cConsulAcquire(lock: ptr CConsulLock, waitMs, leaseMs: int64, output: ptr ptr CConsulLease, error: ptr CError): cint {.importc: "dilock_consul_acquire", header: "dilock/dilock.h", cdecl.}
proc cPostgresAcquire(lock: ptr CPostgresLock, waitMs, leaseMs: int64, output: ptr ptr CPostgresLease, error: ptr CError): cint {.importc: "dilock_postgres_acquire", header: "dilock/dilock.h", cdecl.}
proc cPostgresAcquireDefault(lock: ptr CPostgresLock, output: ptr ptr CPostgresLease, error: ptr CError): cint {.importc: "dilock_postgres_acquire_default", header: "dilock/dilock.h", cdecl.}
proc cRedisTryAcquire(lock: ptr CRedisLock, leaseMs: int64, output: ptr ptr CRedisLease, error: ptr CError): cint {.importc: "dilock_redis_try_acquire", header: "dilock/dilock.h", cdecl.}
proc cEtcdTryAcquire(lock: ptr CEtcdLock, leaseMs: int64, output: ptr ptr CEtcdLease, error: ptr CError): cint {.importc: "dilock_etcd_try_acquire", header: "dilock/dilock.h", cdecl.}
proc cConsulTryAcquire(lock: ptr CConsulLock, leaseMs: int64, output: ptr ptr CConsulLease, error: ptr CError): cint {.importc: "dilock_consul_try_acquire", header: "dilock/dilock.h", cdecl.}
proc cPostgresTryAcquire(lock: ptr CPostgresLock, output: ptr ptr CPostgresLease, error: ptr CError): cint {.importc: "dilock_postgres_try_acquire", header: "dilock/dilock.h", cdecl.}
proc cRedisLeaseValid(lease: ptr CRedisLease): int32 {.importc: "dilock_redis_lease_is_valid", header: "dilock/dilock.h", cdecl.}
proc cEtcdLeaseValid(lease: ptr CEtcdLease): int32 {.importc: "dilock_etcd_lease_is_valid", header: "dilock/dilock.h", cdecl.}
proc cConsulLeaseValid(lease: ptr CConsulLease): int32 {.importc: "dilock_consul_lease_is_valid", header: "dilock/dilock.h", cdecl.}
proc cPostgresLeaseValid(lease: ptr CPostgresLease): int32 {.importc: "dilock_postgres_lease_is_valid", header: "dilock/dilock.h", cdecl.}
proc cRedisLeaseRelease(lease: ptr CRedisLease, outcome: ptr CReleaseOutcome, error: ptr CError): cint {.importc: "dilock_redis_lease_release", header: "dilock/dilock.h", cdecl.}
proc cRedisLeaseRenew(lease: ptr CRedisLease, leaseMs: int64, renewed: ptr int32, error: ptr CError): cint {.importc: "dilock_redis_lease_renew", header: "dilock/dilock.h", cdecl.}
proc cEtcdLeaseRelease(lease: ptr CEtcdLease, outcome: ptr CReleaseOutcome, error: ptr CError): cint {.importc: "dilock_etcd_lease_release", header: "dilock/dilock.h", cdecl.}
proc cConsulLeaseRelease(lease: ptr CConsulLease, outcome: ptr CReleaseOutcome, error: ptr CError): cint {.importc: "dilock_consul_lease_release", header: "dilock/dilock.h", cdecl.}
proc cPostgresLeaseRelease(lease: ptr CPostgresLease, outcome: ptr CReleaseOutcome, error: ptr CError): cint {.importc: "dilock_postgres_lease_release", header: "dilock/dilock.h", cdecl.}
proc cRedisLeaseDestroy(lease: ptr CRedisLease) {.importc: "dilock_redis_lease_destroy", header: "dilock/dilock.h", cdecl.}
proc cEtcdLeaseDestroy(lease: ptr CEtcdLease) {.importc: "dilock_etcd_lease_destroy", header: "dilock/dilock.h", cdecl.}
proc cConsulLeaseDestroy(lease: ptr CConsulLease) {.importc: "dilock_consul_lease_destroy", header: "dilock/dilock.h", cdecl.}
proc cPostgresLeaseDestroy(lease: ptr CPostgresLease) {.importc: "dilock_postgres_lease_destroy", header: "dilock/dilock.h", cdecl.}
proc cCancellationCreate(error: ptr CError): ptr CCancellation {.importc: "dilock_cancellation_create", header: "dilock/dilock.h", cdecl.}
proc cCancellationRequest(value: ptr CCancellation) {.importc: "dilock_cancellation_request", header: "dilock/dilock.h", cdecl.}
proc cCancellationDestroy(value: ptr CCancellation) {.importc: "dilock_cancellation_destroy", header: "dilock/dilock.h", cdecl.}
proc cRedisAcquireCancel(lock: ptr CRedisLock, waitMs, leaseMs: int64, cancellation: ptr CCancellation, output: ptr ptr CRedisLease, error: ptr CError): cint {.importc: "dilock_redis_acquire_with_cancellation", header: "dilock/dilock.h", cdecl.}
proc cEtcdAcquireCancel(lock: ptr CEtcdLock, waitMs, leaseMs: int64, cancellation: ptr CCancellation, output: ptr ptr CEtcdLease, error: ptr CError): cint {.importc: "dilock_etcd_acquire_with_cancellation", header: "dilock/dilock.h", cdecl.}
proc cConsulAcquireDefaultCancel(lock: ptr CConsulLock, cancellation: ptr CCancellation, output: ptr ptr CConsulLease, error: ptr CError): cint {.importc: "dilock_consul_acquire_default_with_cancellation", header: "dilock/dilock.h", cdecl.}
proc cConsulAcquireCancel(lock: ptr CConsulLock, waitMs, leaseMs: int64, cancellation: ptr CCancellation, output: ptr ptr CConsulLease, error: ptr CError): cint {.importc: "dilock_consul_acquire_with_cancellation", header: "dilock/dilock.h", cdecl.}
proc cEtcdLeaseFence(lease: ptr CEtcdLease, fence: ptr CEtcdFence, epoch: cstring, capacity: csize_t, error: ptr CError): cint {.importc: "dilock_etcd_lease_fence", header: "dilock/dilock.h", cdecl.}
proc cConsulLeaseFence(lease: ptr CConsulLease, fence: ptr CConsulFence, epoch: cstring, capacity: csize_t, error: ptr CError): cint {.importc: "dilock_consul_lease_fence", header: "dilock/dilock.h", cdecl.}
proc cConsulLeaseRenew(lease: ptr CConsulLease, renewed: ptr int32, error: ptr CError): cint {.importc: "dilock_consul_lease_renew", header: "dilock/dilock.h", cdecl.}
proc cConsulLeaseTtl(lease: ptr CConsulLease, ttl: ptr int64, error: ptr CError): cint {.importc: "dilock_consul_lease_effective_ttl_ms", header: "dilock/dilock.h", cdecl.}
proc cConsulProvision(options: ptr CConsulOptions, error: ptr CError): cint {.importc: "dilock_consul_provision_namespace", header: "dilock/dilock.h", cdecl.}
proc cPostgresProvision(options: ptr CPostgresOptions, error: ptr CError): cint {.importc: "dilock_postgres_provision_namespace", header: "dilock/dilock.h", cdecl.}
proc cPostgresLeaseRenew(lease: ptr CPostgresLease, renewed: ptr int32, error: ptr CError): cint {.importc: "dilock_postgres_lease_renew", header: "dilock/dilock.h", cdecl.}

proc check(status: cint, error: CError) {.raises: [DilockError].} =
  if status == cint(dsOk): return
  let kind = if status >= 0 and status <= cint(dsInternal): DilockStatus(status) else: dsInternal
  var message = newStringOfCap(error.message.len)
  for character in error.message:
    if character == '\0': break
    message.add character
  var failure = newException(DilockError, message)
  failure.status = kind
  raise failure

proc failInvalidArgument(message: string) {.raises: [DilockError].} =
  var failure = newException(DilockError, message)
  failure.status = dsInvalidArgument
  raise failure

proc validateCStringInput(value, label: string) {.raises: [DilockError].} =
  if '\0' in value:
    failInvalidArgument(label & " cannot contain U+0000 because the C ABI uses NUL-terminated strings")

proc failInternal(message: string) {.raises: [DilockError].} =
  var failure = newException(DilockError, message)
  failure.status = dsInternal
  raise failure

proc requireOutputHandle*[T](handle: ptr T, label: string): ptr T {.raises: [DilockError].} =
  ## Rejects a broken C ABI success result that omitted its owning output.
  ## Exported for the package's contract tests; this is not a stable SDK API.
  if handle.isNil:
    failInternal(label & " creation succeeded without returning a handle")
  handle

template requireHandle(handle, label: untyped) =
  if handle.isNil or handle.raw == nil:
    failInvalidArgument(label & " handle is nil or has been destroyed")

proc releaseOutcomeFromC(value: int32): ReleaseOutcome {.raises: [DilockError].} =
  case value
  of 0: return released
  of 1: return alreadyNotOwned
  of 2: return releaseUnknown
  else: failInternal("linked C ABI returned an invalid release outcome")

proc ensureAbi() {.raises: [DilockError].} =
  if abiVersion() != 1'u32:
    var failure = newException(DilockError, "linked Dilock C ABI version is not 1")
    failure.status = dsInternal
    raise failure

proc connectRedis*(host, namespace, epoch: string, port = 6379'i32,
                   username = "", password = "", database = 0'i32): RedisProvider =
  if port < 0 or port > 65535:
    failInvalidArgument("Redis port must be zero (default) or between 1 and 65535")
  if database < 0:
    failInvalidArgument("Redis database must be non-negative")
  validateCStringInput(host, "Redis host")
  validateCStringInput(username, "Redis username")
  validateCStringInput(password, "Redis password")
  validateCStringInput(namespace, "namespace")
  validateCStringInput(epoch, "coordination epoch")
  ensureAbi()
  var options: CRedisOptions
  cRedisOptionsInit(addr options)
  options.host = host.cstring
  options.port = port
  options.username = username.cstring
  options.password = password.cstring
  options.database = database
  options.name_space = namespace.cstring
  options.coordination_epoch = epoch.cstring
  var raw: ptr CRedisProvider
  var error: CError
  check(cRedisProviderCreate(addr options, addr raw, addr error), error)
  RedisProvider(raw: requireOutputHandle(raw, "Redis provider"))

proc connectEtcd*(endpoint, namespace, epoch: string,
                  autoRenew = false,
                  tlsRootCertificates = "",
                  tlsClientCertificate = "",
                  tlsClientPrivateKey = ""): EtcdProvider =
  ## Connect to etcd using its native Lock API. When `autoRenew` is true,
  ## the C ABI continues the acquisition's keepalive after Lock succeeds.
  ## This is provider-scoped automatic renewal, not a synchronous lease.renew
  ## operation; each acquired etcd lease still owns its own server lease.
  validateCStringInput(endpoint, "etcd endpoint")
  validateCStringInput(namespace, "namespace")
  validateCStringInput(epoch, "coordination epoch")
  validateCStringInput(tlsRootCertificates, "etcd TLS root certificates")
  validateCStringInput(tlsClientCertificate, "etcd TLS client certificate")
  validateCStringInput(tlsClientPrivateKey, "etcd TLS client private key")
  ensureAbi()
  var options: CEtcdOptions
  cEtcdOptionsInit(addr options)
  options.endpoint = endpoint.cstring
  options.name_space = namespace.cstring
  options.coordination_epoch = epoch.cstring
  options.tls_root_certificates = tlsRootCertificates.cstring
  options.tls_client_certificate = tlsClientCertificate.cstring
  options.tls_client_private_key = tlsClientPrivateKey.cstring
  options.auto_renew = if autoRenew: 1'i32 else: 0'i32
  var raw: ptr CEtcdProvider
  var error: CError
  check(cEtcdProviderCreate(addr options, addr raw, addr error), error)
  EtcdProvider(raw: requireOutputHandle(raw, "etcd provider"))

proc connectConsul*(endpoint, namespace, epoch: string, aclToken = ""): ConsulProvider =
  ## Connects to a Consul agent after its matching metadata has been
  ## provisioned. The ACL token is copied into the native provider and is
  ## never included in exception text.
  validateCStringInput(endpoint, "Consul endpoint")
  validateCStringInput(namespace, "namespace")
  validateCStringInput(epoch, "coordination epoch")
  validateCStringInput(aclToken, "Consul ACL token")
  ensureAbi()
  var options: CConsulOptions
  cConsulOptionsInit(addr options)
  options.endpoint = endpoint.cstring
  options.name_space = namespace.cstring
  options.coordination_epoch = epoch.cstring
  options.acl_token = aclToken.cstring
  var raw: ptr CConsulProvider
  var error: CError
  check(cConsulProviderCreate(addr options, addr raw, addr error), error)
  ConsulProvider(raw: requireOutputHandle(raw, "Consul provider"))

proc provisionConsulNamespace*(endpoint, namespace, epoch: string, aclToken = "") =
  ## Explicitly creates compatible Consul namespace metadata using CAS=0.
  ## Normal provider construction never provisions or repairs metadata.
  validateCStringInput(endpoint, "Consul endpoint")
  validateCStringInput(namespace, "namespace")
  validateCStringInput(epoch, "coordination epoch")
  validateCStringInput(aclToken, "Consul ACL token")
  ensureAbi()
  var options: CConsulOptions
  cConsulOptionsInit(addr options)
  options.endpoint = endpoint.cstring
  options.name_space = namespace.cstring
  options.coordination_epoch = epoch.cstring
  options.acl_token = aclToken.cstring
  var error: CError
  check(cConsulProvision(addr options, addr error), error)

proc connectPostgres*(connectionString, namespace, epoch: string): PostgresProvider =
  validateCStringInput(connectionString, "PostgreSQL connection string")
  validateCStringInput(namespace, "namespace")
  validateCStringInput(epoch, "coordination epoch")
  ensureAbi()
  var options: CPostgresOptions
  cPostgresOptionsInit(addr options)
  options.connection_string = connectionString.cstring
  options.name_space = namespace.cstring
  options.coordination_epoch = epoch.cstring
  var raw: ptr CPostgresProvider
  var error: CError
  check(cPostgresProviderCreate(addr options, addr raw, addr error), error)
  PostgresProvider(raw: requireOutputHandle(raw, "PostgreSQL provider"))

proc provisionPostgresNamespace*(connectionString, namespace, epoch: string) =
  validateCStringInput(connectionString, "PostgreSQL connection string")
  validateCStringInput(namespace, "namespace")
  validateCStringInput(epoch, "coordination epoch")
  ensureAbi()
  var options: CPostgresOptions
  cPostgresOptionsInit(addr options)
  options.connection_string = connectionString.cstring
  options.name_space = namespace.cstring
  options.coordination_epoch = epoch.cstring
  var error: CError
  check(cPostgresProvision(addr options, addr error), error)

proc lock*(provider: RedisProvider, resource: string): RedisLock =
  requireHandle(provider, "Redis provider")
  validateCStringInput(resource, "resource")
  var raw: ptr CRedisLock
  var error: CError
  check(cRedisLockCreate(provider.raw, resource.cstring, addr raw, addr error), error)
  RedisLock(raw: requireOutputHandle(raw, "Redis lock"))

proc lock*(provider: EtcdProvider, resource: string): EtcdLock =
  requireHandle(provider, "etcd provider")
  validateCStringInput(resource, "resource")
  var raw: ptr CEtcdLock
  var error: CError
  check(cEtcdLockCreate(provider.raw, resource.cstring, addr raw, addr error), error)
  EtcdLock(raw: requireOutputHandle(raw, "etcd lock"))

proc lock*(provider: ConsulProvider, resource: string): ConsulLock =
  requireHandle(provider, "Consul provider")
  validateCStringInput(resource, "resource")
  var raw: ptr CConsulLock
  var error: CError
  check(cConsulLockCreate(provider.raw, resource.cstring, addr raw, addr error), error)
  ConsulLock(raw: requireOutputHandle(raw, "Consul lock"))

proc lock*(provider: PostgresProvider, resource: string): PostgresLock =
  requireHandle(provider, "PostgreSQL provider")
  validateCStringInput(resource, "resource")
  var raw: ptr CPostgresLock
  var error: CError
  check(cPostgresLockCreate(provider.raw, resource.cstring, addr raw, addr error), error)
  PostgresLock(raw: requireOutputHandle(raw, "PostgreSQL lock"))

proc acquireDefault*(lock: RedisLock): RedisLease =
  requireHandle(lock, "Redis lock")
  var raw: ptr CRedisLease
  var error: CError
  check(cRedisAcquireDefault(lock.raw, addr raw, addr error), error)
  RedisLease(raw: requireOutputHandle(raw, "Redis lease"))

proc acquire*(lock: RedisLock, waitMs, leaseMs: int64): RedisLease =
  requireHandle(lock, "Redis lock")
  var raw: ptr CRedisLease
  var error: CError
  check(cRedisAcquire(lock.raw, waitMs, leaseMs, addr raw, addr error), error)
  RedisLease(raw: requireOutputHandle(raw, "Redis lease"))

proc acquireDefault*(lock: EtcdLock): EtcdLease =
  requireHandle(lock, "etcd lock")
  var raw: ptr CEtcdLease
  var error: CError
  check(cEtcdAcquireDefault(lock.raw, addr raw, addr error), error)
  EtcdLease(raw: requireOutputHandle(raw, "etcd lease"))

proc acquireDefault*(lock: RedisLock, cancellation: Cancellation): RedisLease =
  ## Acquires with the provider's default wait and lease durations while
  ## observing the supplied cancellation handle.
  requireHandle(lock, "Redis lock")
  requireHandle(cancellation, "cancellation")
  var raw: ptr CRedisLease
  var error: CError
  check(cRedisAcquireDefaultCancel(lock.raw, cancellation.raw, addr raw, addr error), error)
  RedisLease(raw: requireOutputHandle(raw, "Redis lease"))

proc acquireDefault*(lock: EtcdLock, cancellation: Cancellation): EtcdLease =
  ## Acquires with the provider's default wait and lease durations while
  ## observing the supplied cancellation handle.
  requireHandle(lock, "etcd lock")
  requireHandle(cancellation, "cancellation")
  var raw: ptr CEtcdLease
  var error: CError
  check(cEtcdAcquireDefaultCancel(lock.raw, cancellation.raw, addr raw, addr error), error)
  EtcdLease(raw: requireOutputHandle(raw, "etcd lease"))

proc acquire*(lock: EtcdLock, waitMs, leaseMs: int64): EtcdLease =
  requireHandle(lock, "etcd lock")
  var raw: ptr CEtcdLease
  var error: CError
  check(cEtcdAcquire(lock.raw, waitMs, leaseMs, addr raw, addr error), error)
  EtcdLease(raw: requireOutputHandle(raw, "etcd lease"))

proc acquireDefault*(lock: ConsulLock): ConsulLease =
  requireHandle(lock, "Consul lock")
  var raw: ptr CConsulLease
  var error: CError
  check(cConsulAcquireDefault(lock.raw, addr raw, addr error), error)
  ConsulLease(raw: requireOutputHandle(raw, "Consul lease"))

proc acquire*(lock: ConsulLock, waitMs, leaseMs: int64): ConsulLease =
  requireHandle(lock, "Consul lock")
  var raw: ptr CConsulLease
  var error: CError
  check(cConsulAcquire(lock.raw, waitMs, leaseMs, addr raw, addr error), error)
  ConsulLease(raw: requireOutputHandle(raw, "Consul lease"))

proc acquireDefault*(lock: PostgresLock): PostgresLease =
  requireHandle(lock, "PostgreSQL lock")
  var raw: ptr CPostgresLease
  var error: CError
  check(cPostgresAcquireDefault(lock.raw, addr raw, addr error), error)
  PostgresLease(raw: requireOutputHandle(raw, "PostgreSQL lease"))

proc acquire*(lock: PostgresLock, waitMs, leaseMs: int64): PostgresLease =
  requireHandle(lock, "PostgreSQL lock")
  var raw: ptr CPostgresLease
  var error: CError
  check(cPostgresAcquire(lock.raw, waitMs, leaseMs, addr raw, addr error), error)
  PostgresLease(raw: requireOutputHandle(raw, "PostgreSQL lease"))

proc tryAcquire*(lock: RedisLock, leaseMs: int64): Option[RedisLease] =
  requireHandle(lock, "Redis lock")
  var raw: ptr CRedisLease
  var error: CError
  let status = cRedisTryAcquire(lock.raw, leaseMs, addr raw, addr error)
  if status == cint(dsBusy): return none(RedisLease)
  check(status, error)
  some(RedisLease(raw: requireOutputHandle(raw, "Redis lease")))

proc tryAcquire*(lock: EtcdLock, leaseMs: int64): Option[EtcdLease] =
  requireHandle(lock, "etcd lock")
  var raw: ptr CEtcdLease
  var error: CError
  check(cEtcdTryAcquire(lock.raw, leaseMs, addr raw, addr error), error)
  some(EtcdLease(raw: requireOutputHandle(raw, "etcd lease")))

proc tryAcquire*(lock: ConsulLock, leaseMs: int64): Option[ConsulLease] =
  requireHandle(lock, "Consul lock")
  var raw: ptr CConsulLease
  var error: CError
  let status = cConsulTryAcquire(lock.raw, leaseMs, addr raw, addr error)
  if status == cint(dsBusy): return none(ConsulLease)
  check(status, error)
  some(ConsulLease(raw: requireOutputHandle(raw, "Consul lease")))

proc tryAcquire*(lock: PostgresLock): Option[PostgresLease] =
  requireHandle(lock, "PostgreSQL lock")
  var raw: ptr CPostgresLease
  var error: CError
  let status = cPostgresTryAcquire(lock.raw, addr raw, addr error)
  if status == cint(dsBusy): return none(PostgresLease)
  check(status, error)
  some(PostgresLease(raw: requireOutputHandle(raw, "PostgreSQL lease")))

proc isValid*(lease: RedisLease): bool =
  requireHandle(lease, "Redis lease")
  cRedisLeaseValid(lease.raw) != 0
proc isValid*(lease: EtcdLease): bool =
  requireHandle(lease, "etcd lease")
  cEtcdLeaseValid(lease.raw) != 0
proc isValid*(lease: ConsulLease): bool =
  requireHandle(lease, "Consul lease")
  cConsulLeaseValid(lease.raw) != 0
proc isValid*(lease: PostgresLease): bool =
  requireHandle(lease, "PostgreSQL lease")
  cPostgresLeaseValid(lease.raw) != 0

proc release*(lease: RedisLease): ReleaseOutcome =
  requireHandle(lease, "Redis lease")
  var rawOutcome: CReleaseOutcome
  var error: CError
  check(cRedisLeaseRelease(lease.raw, addr rawOutcome, addr error), error)
  releaseOutcomeFromC(cast[int32](rawOutcome))

proc release*(lease: EtcdLease): ReleaseOutcome =
  requireHandle(lease, "etcd lease")
  var rawOutcome: CReleaseOutcome
  var error: CError
  check(cEtcdLeaseRelease(lease.raw, addr rawOutcome, addr error), error)
  releaseOutcomeFromC(cast[int32](rawOutcome))

proc release*(lease: ConsulLease): ReleaseOutcome =
  requireHandle(lease, "Consul lease")
  var rawOutcome: CReleaseOutcome
  var error: CError
  check(cConsulLeaseRelease(lease.raw, addr rawOutcome, addr error), error)
  releaseOutcomeFromC(cast[int32](rawOutcome))

proc release*(lease: PostgresLease): ReleaseOutcome =
  requireHandle(lease, "PostgreSQL lease")
  var rawOutcome: CReleaseOutcome
  var error: CError
  check(cPostgresLeaseRelease(lease.raw, addr rawOutcome, addr error), error)
  releaseOutcomeFromC(cast[int32](rawOutcome))

proc renew*(lease: PostgresLease): bool =
  ## Liveness check only; PostgreSQL advisory locks have no server TTL.
  requireHandle(lease, "PostgreSQL lease")
  var renewed: int32
  var error: CError
  check(cPostgresLeaseRenew(lease.raw, addr renewed, addr error), error)
  renewed != 0

proc renew*(lease: ConsulLease): bool =
  ## Renews the same Consul session and confirms the same owner and LockIndex.
  requireHandle(lease, "Consul lease")
  var renewed: int32
  var error: CError
  check(cConsulLeaseRenew(lease.raw, addr renewed, addr error), error)
  renewed != 0

proc effectiveTtlMs*(lease: ConsulLease): int64 =
  requireHandle(lease, "Consul lease")
  var ttl: int64
  var error: CError
  check(cConsulLeaseTtl(lease.raw, addr ttl, addr error), error)
  ttl

proc renew*(lease: RedisLease, leaseMs: int64): bool =
  ## Extends a Redis lease when the backend still recognizes its owner token.
  ## Backend errors and lost ownership raise `DilockError`; they are not reported
  ## as a normal false result. Renewal is unavailable for etcd through this ABI.
  requireHandle(lease, "Redis lease")
  var renewed: int32
  var error: CError
  check(cRedisLeaseRenew(lease.raw, leaseMs, addr renewed, addr error), error)
  renewed != 0

proc destroy*(provider: RedisProvider) =
  if not provider.isNil and provider.raw != nil: cRedisProviderDestroy(provider.raw); provider.raw = nil
proc destroy*(provider: EtcdProvider) =
  if not provider.isNil and provider.raw != nil: cEtcdProviderDestroy(provider.raw); provider.raw = nil
proc destroy*(provider: ConsulProvider) =
  if not provider.isNil and provider.raw != nil: cConsulProviderDestroy(provider.raw); provider.raw = nil
proc destroy*(provider: PostgresProvider) =
  if not provider.isNil and provider.raw != nil: cPostgresProviderDestroy(provider.raw); provider.raw = nil
proc destroy*(lock: RedisLock) =
  if not lock.isNil and lock.raw != nil: cRedisLockDestroy(lock.raw); lock.raw = nil
proc destroy*(lock: EtcdLock) =
  if not lock.isNil and lock.raw != nil: cEtcdLockDestroy(lock.raw); lock.raw = nil
proc destroy*(lock: ConsulLock) =
  if not lock.isNil and lock.raw != nil: cConsulLockDestroy(lock.raw); lock.raw = nil
proc destroy*(lock: PostgresLock) =
  if not lock.isNil and lock.raw != nil: cPostgresLockDestroy(lock.raw); lock.raw = nil
proc destroy*(lease: RedisLease) =
  if not lease.isNil and lease.raw != nil: cRedisLeaseDestroy(lease.raw); lease.raw = nil
proc destroy*(lease: EtcdLease) =
  if not lease.isNil and lease.raw != nil: cEtcdLeaseDestroy(lease.raw); lease.raw = nil
proc destroy*(lease: ConsulLease) =
  if not lease.isNil and lease.raw != nil: cConsulLeaseDestroy(lease.raw); lease.raw = nil
proc destroy*(lease: PostgresLease) =
  if not lease.isNil and lease.raw != nil: cPostgresLeaseDestroy(lease.raw); lease.raw = nil

proc initCancellation*(): Cancellation =
  ensureAbi()
  var error: CError
  let raw = cCancellationCreate(addr error)
  if raw == nil: check(error.code, error)
  Cancellation(raw: requireOutputHandle(raw, "cancellation"))
proc request*(cancellation: Cancellation) =
  requireHandle(cancellation, "cancellation")
  cCancellationRequest(cancellation.raw)
proc destroy*(cancellation: Cancellation) =
  if not cancellation.isNil and cancellation.raw != nil: cCancellationDestroy(cancellation.raw); cancellation.raw = nil

proc acquire*(lock: RedisLock, waitMs, leaseMs: int64, cancellation: Cancellation): RedisLease =
  requireHandle(lock, "Redis lock")
  requireHandle(cancellation, "cancellation")
  var raw: ptr CRedisLease
  var error: CError
  check(cRedisAcquireCancel(lock.raw, waitMs, leaseMs, cancellation.raw, addr raw, addr error), error)
  RedisLease(raw: requireOutputHandle(raw, "Redis lease"))

proc acquire*(lock: EtcdLock, waitMs, leaseMs: int64, cancellation: Cancellation): EtcdLease =
  requireHandle(lock, "etcd lock")
  requireHandle(cancellation, "cancellation")
  var raw: ptr CEtcdLease
  var error: CError
  check(cEtcdAcquireCancel(lock.raw, waitMs, leaseMs, cancellation.raw, addr raw, addr error), error)
  EtcdLease(raw: requireOutputHandle(raw, "etcd lease"))

proc acquireDefault*(lock: ConsulLock, cancellation: Cancellation): ConsulLease =
  requireHandle(lock, "Consul lock")
  requireHandle(cancellation, "cancellation")
  var raw: ptr CConsulLease
  var error: CError
  check(cConsulAcquireDefaultCancel(lock.raw, cancellation.raw, addr raw, addr error), error)
  ConsulLease(raw: requireOutputHandle(raw, "Consul lease"))

proc acquire*(lock: ConsulLock, waitMs, leaseMs: int64, cancellation: Cancellation): ConsulLease =
  requireHandle(lock, "Consul lock")
  requireHandle(cancellation, "cancellation")
  var raw: ptr CConsulLease
  var error: CError
  check(cConsulAcquireCancel(lock.raw, waitMs, leaseMs, cancellation.raw, addr raw, addr error), error)
  ConsulLease(raw: requireOutputHandle(raw, "Consul lease"))

proc fence*(lease: EtcdLease): EtcdFence =
  requireHandle(lease, "etcd lease")
  var rawFence: CEtcdFence
  var error: CError
  let probeStatus = cEtcdLeaseFence(lease.raw, addr rawFence, nil, 0, addr error)
  if probeStatus != cint(dsInvalidArgument):
    if probeStatus == cint(dsOk): failInternal("C ABI fence size probe unexpectedly succeeded")
    check(probeStatus, error)
  if rawFence.coordination_epoch_bytes > csize_t(high(int) - 1):
    failInternal("C ABI returned an unrepresentable fencing epoch length")
  let epochLength = int(rawFence.coordination_epoch_bytes)
  var storage = newString(epochLength + 1)
  check(cEtcdLeaseFence(lease.raw, addr rawFence, cast[cstring](addr storage[0]),
                        csize_t(storage.len), addr error), error)
  storage.setLen(epochLength)
  EtcdFence(epoch: storage, createRevision: rawFence.create_revision)

proc fence*(lease: ConsulLease): ConsulFence =
  requireHandle(lease, "Consul lease")
  var rawFence: CConsulFence
  var error: CError
  let probeStatus = cConsulLeaseFence(lease.raw, addr rawFence, nil, 0, addr error)
  if probeStatus != cint(dsInvalidArgument):
    if probeStatus == cint(dsOk): failInternal("C ABI fence size probe unexpectedly succeeded")
    check(probeStatus, error)
  if rawFence.coordination_epoch_bytes > csize_t(high(int) - 1):
    failInternal("C ABI returned an unrepresentable fencing epoch length")
  let epochLength = int(rawFence.coordination_epoch_bytes)
  var storage = newString(epochLength + 1)
  check(cConsulLeaseFence(lease.raw, addr rawFence, cast[cstring](addr storage[0]),
                          csize_t(storage.len), addr error), error)
  storage.setLen(epochLength)
  ConsulFence(epoch: storage, lockIndex: rawFence.lock_index)

template withLease*(leaseName, acquireExpression: untyped, body: untyped) =
  block:
    let leaseName = acquireExpression
    try:
      body
    finally:
      leaseName.destroy()
