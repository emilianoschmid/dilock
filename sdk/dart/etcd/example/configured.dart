import 'package:dilock/dilock.dart';
import 'package:dilock_etcd/dilock_etcd.dart';

Future<void> main() async {
  final provider = await EtcdProvider.connect(
    'https://etcd.example:2379',
    options: EtcdProviderOptions(
      namespace: 'billing',
      coordinationEpoch: 'billing-cluster-incarnation-7',
      rpcTimeout: const Duration(seconds: 3),
      cleanupTimeout: const Duration(seconds: 2),
      defaultAcquisition: const AcquireOptions(
        wait: Duration(seconds: 8),
        lease: LeaseOptions(
          leaseDuration: Duration(seconds: 45),
          rpcTimeout: Duration(seconds: 3),
          cleanupTimeout: Duration(seconds: 2),
          autoRenew: true,
          requireFencing: true,
        ),
      ),
      cleanupObserver: _reportCleanup,
    ),
  );
  try {
    const policy = AcquireOptions(
      wait: Duration(seconds: 8),
      lease: LeaseOptions(
        leaseDuration: Duration(seconds: 45),
        rpcTimeout: Duration(seconds: 3),
        cleanupTimeout: Duration(seconds: 2),
        autoRenew: true,
        requireFencing: true,
      ),
    );
    await provider.lock('invoice/8391').withLeaseOptions<void>(
      policy,
      (lease) async {
        lease.ensureHeld();
        // Persist result.fence atomically with the protected data update.
        final fence = lease.fence;
        print('enforce ${fence.coordinationEpoch}/${fence.createRevision}');
      },
    );
  } finally {
    await provider.close();
  }
}

void _reportCleanup(CleanupObservation observation) {
  print(
    '${observation.backend}: ${observation.outcome.name}: ${observation.reason}',
  );
}
