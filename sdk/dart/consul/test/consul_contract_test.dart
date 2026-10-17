import 'dart:convert';
import 'dart:io';

import 'package:dilock/dilock.dart';
import 'package:dilock_consul/dilock_consul.dart';
import 'package:test/test.dart';

void main() {
  test(
    'rejects malformed Consul options before opening a connection',
    () async {
      await expectLater(
        ConsulProvider.connect(
          'http://127.0.0.1:8500/base',
          options: const ConsulProviderOptions(coordinationEpoch: 'epoch-a'),
        ),
        throwsA(
          isA<DilockException>().having(
            (error) => error.kind,
            'kind',
            FailureKind.invalidOptions,
          ),
        ),
      );
      await expectLater(
        ConsulProvider.connect(
          'http://127.0.0.1:8500',
          options: const ConsulProviderOptions(
            coordinationEpoch: 'epoch-a',
            token: 'bad token',
          ),
        ),
        throwsA(
          isA<DilockException>().having(
            (error) => error.kind,
            'kind',
            FailureKind.invalidOptions,
          ),
        ),
      );
    },
  );

  test(
    'provisions only absent metadata, verifies owner and returns retained LockIndex',
    () async {
      var metadata = <String, Object?>{
        'protocol_major': 1,
        'identity_scheme': 'sha256-framed-utf8-v1',
        'profile': 'consul-kv-session-v1',
        'coordination_epoch': 'test-epoch',
        'fixed_lease_ms': null,
      };
      String? owner;
      var lockIndex = 0;
      var sessionSequence = 0;
      var failNextDestroy = false;
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final serving = server.forEach((request) async {
        final response = request.response;
        response.headers.contentType = ContentType.json;
        final path = request.uri.path;
        if (request.method == 'PUT' &&
            path == '/v1/kv/dilock/namespaces/default/meta') {
          metadata = jsonDecode(await utf8.decoder.bind(request).join())
              as Map<String, Object?>;
          response.write('true');
        } else if (request.method == 'GET' &&
            path == '/v1/kv/dilock/namespaces/default/meta') {
          response.write(
            jsonEncode([
              {
                'Key': path.substring('/v1/kv/'.length),
                'Value': base64Encode(utf8.encode(jsonEncode(metadata))),
              },
            ]),
          );
        } else if (request.method == 'PUT' && path == '/v1/session/create') {
          sessionSequence++;
          response.write(jsonEncode({'ID': 'session-$sessionSequence'}));
        } else if (request.method == 'PUT' &&
            path ==
                '/v1/kv/dilock/locks/${resourceId('default', 'invoice/42')}') {
          if (request.uri.queryParameters.containsKey('acquire')) {
            if (owner == null) {
              owner = request.uri.queryParameters['acquire'];
              lockIndex++;
              response.write('true');
            } else {
              response.write('false');
            }
          } else if (request.uri.queryParameters.containsKey('release')) {
            if (owner == request.uri.queryParameters['release']) owner = null;
            response.write('true');
          } else {
            response.statusCode = HttpStatus.badRequest;
            response.write('{}');
          }
        } else if (request.method == 'GET' &&
            path.startsWith('/v1/kv/dilock/locks/')) {
          response.write(
            jsonEncode([
              {
                'Key': path.substring('/v1/kv/'.length),
                'Session': owner,
                'LockIndex': lockIndex,
              },
            ]),
          );
        } else if (request.method == 'PUT' &&
            path.startsWith('/v1/session/renew/')) {
          response.write(
            jsonEncode([
              {'ID': path.split('/').last},
            ]),
          );
        } else if (request.method == 'PUT' &&
            path.startsWith('/v1/session/destroy/')) {
          owner = null;
          final acknowledged = failNextDestroy ? false : true;
          failNextDestroy = false;
          response.write(jsonEncode(acknowledged));
        } else {
          response.statusCode = HttpStatus.notFound;
          response.write('{}');
        }
        await response.close();
      });
      final endpoint = 'http://127.0.0.1:${server.port}';
      const options = ConsulProviderOptions(coordinationEpoch: 'test-epoch');
      try {
        await ConsulProvider.provisionNamespace(endpoint, options: options);
        final provider = await ConsulProvider.connect(
          endpoint,
          options: options,
        );
        final lease = await provider.lock('invoice/42').acquire(
              options: const AcquireOptions(
                wait: Duration(seconds: 1),
                lease: LeaseOptions(leaseDuration: Duration(seconds: 10)),
              ),
            );
        expect(lease.fence.coordinationEpoch, 'test-epoch');
        expect(lease.fence.lockIndex, 1);
        expect(await lease.release(), ReleaseOutcome.released);
        final destroyUnconfirmedLease =
            await provider.lock('invoice/42').acquire(
                  options: const AcquireOptions(
                    wait: Duration(seconds: 1),
                    lease: LeaseOptions(leaseDuration: Duration(seconds: 10)),
                  ),
                );
        failNextDestroy = true;
        expect(await destroyUnconfirmedLease.release(), ReleaseOutcome.unknown);
        await provider.close();

        metadata['future_field'] = 'unsupported';
        await expectLater(
          ConsulProvider.connect(endpoint, options: options),
          throwsA(
            isA<DilockException>().having(
              (error) => error.kind,
              'kind',
              FailureKind.protocolMismatch,
            ),
          ),
        );
      } finally {
        await server.close(force: true);
        await serving;
      }
    },
  );

  test('rejects a lease shorter than Consul session minimum', () async {
    await expectLater(
      ConsulProvider.connect(
        'http://127.0.0.1:8500',
        options: const ConsulProviderOptions(
          coordinationEpoch: 'epoch-a',
          defaultAcquisition: AcquireOptions(
            lease: LeaseOptions(leaseDuration: Duration(seconds: 5)),
          ),
        ),
      ),
      throwsA(
        isA<DilockException>().having(
          (error) => error.kind,
          'kind',
          FailureKind.invalidOptions,
        ),
      ),
    );
  });
}
