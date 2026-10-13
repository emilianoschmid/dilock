import 'package:dilock_etcd/dilock_etcd.dart';
import 'package:dilock/dilock.dart';

Future<void> main() async {
  final provider = await EtcdProvider.connect(
    'http://127.0.0.1:2379',
    options: const EtcdProviderOptions(
      coordinationEpoch: 'local-etcd-incarnation-1',
    ),
  );
  try {
    // Handles identify resources; they reuse this provider's gRPC channel.
    await provider.lock('reports/daily').withLease<void>((lease) async {
      lease.ensureHeld();
      print(
        'fence: ${lease.fence.coordinationEpoch}/${lease.fence.createRevision}',
      );
    });
    const explicit = AcquireOptions(
      wait: Duration(seconds: 3),
      lease: LeaseOptions(
        leaseDuration: Duration(seconds: 20),
        rpcTimeout: Duration(seconds: 1),
        cleanupTimeout: Duration(seconds: 1),
        requireFencing: true,
      ),
    );
    await provider.lock('reports/weekly').withLeaseOptions<void>(explicit, (
      lease,
    ) async {
      lease.ensureHeld();
      print(
        'fence: ${lease.fence.coordinationEpoch}/${lease.fence.createRevision}',
      );
    });
  } finally {
    await provider.close();
  }
}
