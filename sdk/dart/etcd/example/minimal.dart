import 'package:dilock_etcd/dilock_etcd.dart';

Future<void> main() async {
  final provider = await EtcdProvider.connect(
    'http://127.0.0.1:2379',
    options: const EtcdProviderOptions(
      coordinationEpoch: 'local-etcd-incarnation-1',
    ),
  );
  try {
    await provider.lock('reports/daily').withLease<void>((lease) async {
      lease.ensureHeld();
      print(
        'fence: ${lease.fence.coordinationEpoch}/${lease.fence.createRevision}',
      );
    });
  } finally {
    await provider.close();
  }
}
