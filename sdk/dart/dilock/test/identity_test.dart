import 'package:dilock/dilock.dart';
import 'package:test/test.dart';

void main() {
  test('matches shared identity and backend-name vectors', () {
    const id =
        'e0cdd484a99b6aee4162335939d2a2d128a490c097069fd85b2fc1e540a940b6';
    expect(resourceId('default', 'camera/123/reindex'), id);
    expect(redisKey('default', 'camera/123/reindex'), 'dilock:{$id}:lease');
    expect(etcdLockName('default', 'camera/123/reindex'), '/dilock/locks/$id');
  });

  test('does not normalize composed Unicode resource names', () {
    expect(resourceId('default', 'café'),
        isNot(resourceId('default', 'cafe\u0301')));
  });

  test('rejects invalid namespace and resource bytes', () {
    expect(() => resourceId('Production', 'resource'),
        throwsA(isA<InvalidIdentityException>()));
    expect(() => resourceId('default', ''),
        throwsA(isA<InvalidIdentityException>()));
    expect(() => resourceId('default', 'a\u0000b'),
        throwsA(isA<InvalidIdentityException>()));
    expect(() => resourceId('default', List<String>.filled(1025, 'x').join()),
        throwsA(isA<InvalidIdentityException>()));
    expect(() => resourceId('dēfault', 'x'),
        throwsA(isA<InvalidIdentityException>()));
    expect(
      () => resourceId('default', String.fromCharCode(0xd800)),
      throwsA(isA<InvalidIdentityException>()),
    );
    expect(
      () => resourceId('default', String.fromCharCode(0xdc00)),
      throwsA(isA<InvalidIdentityException>()),
    );
    expect(
      () => resourceId('default', '${String.fromCharCode(0xd800)}x'),
      throwsA(isA<InvalidIdentityException>()),
    );
    final supplementary = String.fromCharCode(0x1f600);
    expect(resourceId('default', supplementary), hasLength(64));
    expect(resourceId('default', supplementary),
        isNot(resourceId('default', '\uFFFD')));
  });

  test(
      'shared default durations validate and explicit policies reject omissions',
      () {
    const defaults = AcquireOptions();
    expect(defaults.wait, const Duration(seconds: 5));
    expect(defaults.lease.leaseDuration, const Duration(seconds: 30));
    expect(defaults.lease.rpcTimeout, const Duration(seconds: 2));
    expect(defaults.lease.cleanupTimeout, const Duration(seconds: 2));
    defaults.validate();
    expect(
      () => const LeaseOptions(rpcTimeout: Duration(seconds: 30)).validate(),
      throwsA(isA<DilockException>()
          .having((error) => error.kind, 'kind', FailureKind.invalidOptions)),
    );
  });
}
