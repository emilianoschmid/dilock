import std/unittest
import dilock

suite "Dilock Nim C ABI binding":
  test "linked ABI version is the expected local draft":
    check abiVersion() == 1'u32

  test "status taxonomy preserves contention and unsupported capability":
    check ord(dsBusy) == 1
    check ord(dsWaitTimeout) == 4
    check ord(dsUnsupportedCapability) == 9

  test "release result values match C ABI v1":
    check ord(released) == 0
    check ord(alreadyNotOwned) == 1
    check ord(releaseUnknown) == 2

  test "Consul provider rejects NUL before calling the C ABI":
    var rejected = false
    try:
      discard connectConsul("http://127.0.0.1:8500\0ignored", "default", "epoch")
    except DilockError as error:
      check error.status == dsInvalidArgument
      rejected = true
    check rejected

  test "successful ABI calls without an owning handle fail closed":
    var value = 7
    check requireOutputHandle(addr value, "test handle") == addr value

    var rejected = false
    try:
      discard requireOutputHandle(cast[ptr int](nil), "test handle")
    except DilockError as error:
      check error.status == dsInternal
      rejected = true
    check rejected

  test "C-string connection inputs reject embedded NUL before calling the ABI":
    var redisRejected = false
    try:
      discard connectRedis("127.0.0.1\0ignored", "default", "epoch")
    except DilockError as error:
      check error.status == dsInvalidArgument
      redisRejected = true
    check redisRejected

    var etcdRejected = false
    try:
      discard connectEtcd("http://127.0.0.1\0ignored", "default", "epoch")
    except DilockError as error:
      check error.status == dsInvalidArgument
      etcdRejected = true
    check etcdRejected

  test "etcd TLS PEM inputs reject embedded NUL before calling the ABI":
    var rejected = 0
    try:
      discard connectEtcd("127.0.0.1:2379", "default", "epoch",
                          tlsRootCertificates = "root\0ignored")
    except DilockError as error:
      check error.status == dsInvalidArgument
      inc rejected
    try:
      discard connectEtcd("127.0.0.1:2379", "default", "epoch",
                          tlsClientCertificate = "cert\0ignored")
    except DilockError as error:
      check error.status == dsInvalidArgument
      inc rejected
    try:
      discard connectEtcd("127.0.0.1:2379", "default", "epoch",
                          tlsClientPrivateKey = "key\0ignored")
    except DilockError as error:
      check error.status == dsInvalidArgument
      inc rejected
    check rejected == 3

  test "Redis connection rejects negative database and out-of-range port before the ABI":
    var rejected = 0
    try:
      discard connectRedis("127.0.0.1", "default", "epoch", database = -1)
    except DilockError as error:
      check error.status == dsInvalidArgument
      inc rejected
    try:
      discard connectRedis("127.0.0.1", "default", "epoch", port = 65536)
    except DilockError as error:
      check error.status == dsInvalidArgument
      inc rejected
    check rejected == 2

  test "destroy accepts nil handles and use of a nil handle is reported":
    var redisProvider: RedisProvider
    var etcdProvider: EtcdProvider
    var consulProvider: ConsulProvider
    var redisLock: RedisLock
    var etcdLock: EtcdLock
    var redisLease: RedisLease
    var etcdLease: EtcdLease
    var consulLock: ConsulLock
    var consulLease: ConsulLease
    var cancellation: Cancellation

    redisProvider.destroy()
    etcdProvider.destroy()
    consulProvider.destroy()
    redisLock.destroy()
    etcdLock.destroy()
    consulLock.destroy()
    redisLease.destroy()
    etcdLease.destroy()
    consulLease.destroy()
    cancellation.destroy()

    var rejected = 0
    try:
      discard redisProvider.lock("resource")
    except DilockError as error:
      check error.status == dsInvalidArgument
      inc rejected
    try:
      discard etcdProvider.lock("resource")
    except DilockError as error:
      check error.status == dsInvalidArgument
      inc rejected
    try:
      discard consulProvider.lock("resource")
    except DilockError as error:
      check error.status == dsInvalidArgument
      inc rejected
    try:
      discard redisLock.acquireDefault()
    except DilockError as error:
      check error.status == dsInvalidArgument
      inc rejected
    try:
      discard etcdLock.acquireDefault()
    except DilockError as error:
      check error.status == dsInvalidArgument
      inc rejected
    try:
      discard redisLock.acquireDefault(cancellation)
    except DilockError as error:
      check error.status == dsInvalidArgument
      inc rejected
    try:
      discard etcdLock.acquireDefault(cancellation)
    except DilockError as error:
      check error.status == dsInvalidArgument
      inc rejected
    try:
      discard redisLease.isValid()
    except DilockError as error:
      check error.status == dsInvalidArgument
      inc rejected
    try:
      discard etcdLease.release()
    except DilockError as error:
      check error.status == dsInvalidArgument
      inc rejected
    try:
      discard consulLease.release()
    except DilockError as error:
      check error.status == dsInvalidArgument
      inc rejected
    try:
      discard redisLease.renew(1000)
    except DilockError as error:
      check error.status == dsInvalidArgument
      inc rejected
    try:
      cancellation.request()
    except DilockError as error:
      check error.status == dsInvalidArgument
      inc rejected
    check rejected == 12
