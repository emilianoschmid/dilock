import 'package:dilock/dilock.dart';
import 'package:dilock_etcd/dilock_etcd.dart';
import 'package:test/test.dart';

void main() {
  test('etcd lock name uses the shared cross-language identity vector', () {
    const id =
        'e0cdd484a99b6aee4162335939d2a2d128a490c097069fd85b2fc1e540a940b6';
    expect(etcdLockName('default', 'camera/123/reindex'), '/dilock/locks/$id');
  });

  test('fence is scoped by the operator-approved coordination epoch', () {
    const first = EtcdFence('cluster-incarnation-a', 42);
    const restored = EtcdFence('cluster-incarnation-b', 42);
    expect(first.createRevision, restored.createRevision);
    expect(first.coordinationEpoch, isNot(restored.coordinationEpoch));
  });

  test('complete lease policy preserves its fencing requirement', () {
    const policy = LeaseOptions(requireFencing: true);
    expect(policy.validate, returnsNormally);
    expect(policy.requireFencing, isTrue);
  });
}
