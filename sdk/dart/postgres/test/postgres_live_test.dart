import 'dart:io';

import 'package:dilock/dilock.dart';
import 'package:dilock_postgres/dilock_postgres.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

void main() {
  test('PostgreSQL route contends, renews and hands off', () async {
    final portValue = Platform.environment['DILOCK_POSTGRES_PORT'];
    if (portValue == null || portValue.isEmpty) return;

    final endpoint = Endpoint(
      host: '127.0.0.1',
      port: int.parse(portValue),
      database: 'dilock',
      username: 'postgres',
      password: 'postgres',
    );
    final options = PostgresProviderOptions(
      endpoint: endpoint,
      namespace: 'dart-postgres-live',
      coordinationEpoch: 'dart-postgres-live-epoch',
      defaultAcquisition: AcquireOptions(
        wait: const Duration(milliseconds: 500),
        lease: LeaseOptions(leaseDuration: const Duration(seconds: 30)),
      ),
    );
    await PostgresProvider.provisionNamespace(options: options);
    final first = await PostgresProvider.connect(options: options);
    final second = await PostgresProvider.connect(options: options);

    final firstLock = first.lock('shared');
    final secondLock = second.lock('shared');
    final otherLock = second.lock('other');
    final policy = options.defaultAcquisition;

    final firstLease = await firstLock.acquire(options: policy);
    expect(await secondLock.tryAcquire(options: policy.lease), isNull);
    final otherLease = await otherLock.acquire(options: policy);
    expect(await firstLease.renew(), isTrue);
    expect(await otherLease.release(), ReleaseOutcome.released);
    expect(await firstLease.release(), ReleaseOutcome.released);
    final successor = await secondLock.acquire(options: policy);
    expect(await successor.release(), ReleaseOutcome.released);
    await first.close();
    await second.close();
  });
}
