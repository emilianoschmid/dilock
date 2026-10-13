// Local-only Dart example. No TTL, network, renewal, wire token or fencing.
enum ReleaseOutcome { released, alreadyNotOwned }

/// Single isolate/event-loop fixture; mutations do not suspend.
final class MemoryStore {
  final Map<String, Object> _owners = {};
  bool claim(String resource, Object owner) {
    if (_owners.containsKey(resource)) return false;
    _owners[resource] = owner;
    return true;
  }
  ReleaseOutcome release(String resource, Object owner) {
    if (!identical(_owners[resource], owner)) return ReleaseOutcome.alreadyNotOwned;
    _owners.remove(resource);
    return ReleaseOutcome.released;
  }
  void expireForTest(String resource) { _owners.remove(resource); }
}

/// One acquisition; release must be awaited, never delegated to GC.
final class Lease {
  final MemoryStore _store;
  final String _resource;
  final Object _owner;
  bool _closed = false;
  Lease._(this._store, this._resource, this._owner);
  Future<ReleaseOutcome> release() async {
    if (_closed) return ReleaseOutcome.alreadyNotOwned;
    _closed = true;
    return _store.release(_resource, _owner);
  }
}
final class ResourceLock {
  final MemoryStore _store;
  final String _resource;
  ResourceLock._(this._store, this._resource) {
    if (_resource.isEmpty) throw ArgumentError('empty resource');
  }
  Future<Lease?> tryAcquire() async {
    final owner = Object();
    return _store.claim(_resource, owner) ? Lease._(_store, _resource, owner) : null;
  }
  Future<T> withLease<T>(Future<T> Function(Lease) body) async {
    final lease = await tryAcquire();
    if (lease == null) throw StateError('busy');
    try { return await body(lease); }
    finally { await lease.release(); }
  }
}
final class Provider {
  final MemoryStore _store;
  Provider(this._store);
  ResourceLock lock(String resource) => ResourceLock._(_store, resource);
}
void check(bool value) {
  if (!value) throw StateError('style assertion failed');
}
Future<void> main() async {
  final store = MemoryStore();
  final resource = Provider(store).lock('example/resource');
  final first = await resource.tryAcquire();
  check(first != null && await resource.tryAcquire() == null);
  store.expireForTest('example/resource');
  final second = await resource.tryAcquire();
  check(second != null);
  check(await first!.release() == ReleaseOutcome.alreadyNotOwned);
  check(await resource.tryAcquire() == null);
  check(await second!.release() == ReleaseOutcome.released);
  check(await second.release() == ReleaseOutcome.alreadyNotOwned);
  final failure = Exception('consumer failure');
  try { await resource.withLease<void>((_) async { throw failure; }); }
  catch (error) { check(identical(error, failure)); }
  check(await resource.withLease<int>((_) async => 42) == 42);
  print('PASS dart: busy, stale owner, idempotence, awaited finally');
}
