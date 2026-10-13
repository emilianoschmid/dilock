import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:dilock/dilock.dart';
import 'package:dilock_redis/dilock_redis.dart';
// ignore: implementation_imports
import 'package:dilock_redis/src/provider.dart';
import 'package:test/test.dart';

void main() {
  late _FakeRedis server;
  late RedisProvider provider;
  late List<CleanupObservation> cleanupObservations;

  setUp(() async {
    server = await _FakeRedis.start();
    cleanupObservations = <CleanupObservation>[];
    provider = await RedisProvider.connect(
      'redis://127.0.0.1:${server.port}/',
      options: RedisProviderOptions(
        coordinationEpoch: 'test-epoch',
        cleanupObserver: cleanupObservations.add,
      ),
    );
  });

  tearDown(() async {
    await provider.close();
    await server.close();
  });

  test('acknowledged contention, release, and successor are distinct states',
      () async {
    final handle = provider.lock('reports/daily');
    final first = await handle.tryAcquire();
    expect(first, isNotNull);
    expect(await handle.tryAcquire(), isNull);
    expect(first!.state, LeaseState.held);
    expect(await first.release(), ReleaseOutcome.released);

    final successor = await handle.tryAcquire();
    expect(successor, isNotNull);
    expect(await successor!.release(), ReleaseOutcome.released);
  });

  test('concurrent close calls wait for the same active-operation shutdown',
      () async {
    final metadataRequestStarted = Completer<void>();
    server.metadataRequestStarted = metadataRequestStarted;
    server.delayMetadataResponse = const Duration(milliseconds: 180);
    final acquisition = provider.lock('shutdown/in-flight').tryAcquire();
    await metadataRequestStarted.future;

    final firstClose = provider.close();
    var secondCloseCompleted = false;
    final secondClose = provider.close().then((_) {
      secondCloseCompleted = true;
    });
    await Future<void>.delayed(const Duration(milliseconds: 20));
    expect(secondCloseCompleted, isFalse,
        reason: 'every close caller must wait for transport shutdown');

    await Future.wait<void>(<Future<void>>[firstClose, secondClose]);
    await acquisition;
    expect(secondCloseCompleted, isTrue);
    expect(server.heldKeys, isEmpty);
  });

  test('contention verification shares the remaining acquire wait budget',
      () async {
    final handle = provider.lock('budget/type-timeout');
    final owner = await handle.tryAcquire();
    expect(owner, isNotNull);
    server.delayMetadataResponse = const Duration(milliseconds: 100);
    server.delayTypeResponse = const Duration(milliseconds: 100);
    final stopwatch = Stopwatch()..start();

    await expectLater(
      handle.acquire(
        const Duration(milliseconds: 150),
        options: const LeaseOptions(
          leaseDuration: Duration(seconds: 1),
          rpcTimeout: Duration(milliseconds: 500),
        ),
      ),
      throwsA(isA<DilockException>().having(
        (error) => error.kind,
        'kind',
        FailureKind.waitTimeout,
      )),
    );
    expect(stopwatch.elapsed, lessThan(const Duration(milliseconds: 190)));
    await owner!.release();
  });

  test('cancellation during contention TYPE remains known cancellation',
      () async {
    final handle = provider.lock('cancel/type');
    final owner = await handle.tryAcquire();
    expect(owner, isNotNull);
    server.delayTypeResponse = const Duration(milliseconds: 250);
    final token = CancellationToken();
    final attempt = handle.tryAcquire(cancellationToken: token);
    await Future<void>.delayed(const Duration(milliseconds: 25));
    token.cancel();

    await expectLater(
      attempt,
      throwsA(isA<DilockException>()
          .having((error) => error.kind, 'kind', FailureKind.cancelled)
          .having((error) => error.outcomeUnknown, 'outcomeUnknown', isFalse)),
    );
    await owner!.release();
  });

  test('two resources use one provider and scope cleanup releases both',
      () async {
    final daily = provider.lock('reports/daily');
    final weekly = provider.lock('reports/weekly');
    final connectedBeforeHandles = server.acceptedConnections;
    expect(connectedBeforeHandles, greaterThan(0));

    final barrier = Completer<void>();
    var entered = 0;
    Future<void> work(RedisLease lease) async {
      lease.ensureHeld();
      entered++;
      if (entered == 2) barrier.complete();
      await barrier.future;
    }

    await Future.wait<void>(<Future<void>>[
      daily.withLease<void>(work),
      weekly.withLease<void>(work),
    ]);
    expect(entered, 2);
    expect(server.heldKeys, isEmpty);
  });

  test('scope fails when Redis no longer reports this owner at cleanup',
      () async {
    await expectLater(
      provider.lock('scope/owner-lost').withLease<void>((_) async {
        server.clearHeldLocks();
      }),
      throwsA(isA<DilockException>().having(
        (error) => error.kind,
        'kind',
        FailureKind.leaseLost,
      )),
    );
  });

  test('successful scope reports an unknown Redis release acknowledgement',
      () async {
    await expectLater(
      provider.lock('scope/release-unknown').withLease<void>((_) async {
        server.failNextOwnerCleanup = true;
      }),
      throwsA(isA<DilockException>()
          .having((error) => error.kind, 'kind', FailureKind.releaseUnknown)
          .having((error) => error.outcomeUnknown, 'outcomeUnknown', isTrue)),
    );
    expect(cleanupObservations.last.outcome, ReleaseOutcome.unknown);
  });

  test(
      'cancelled dispatched SET is unknown and tracked owner cleanup removes it',
      () async {
    server.delaySetResponse = true;
    final token = CancellationToken();
    final handle = provider.lock('cancelled/acquisition');
    final acquisition = handle.tryAcquire(cancellationToken: token);
    await Future<void>.delayed(const Duration(milliseconds: 30));
    token.cancel();

    await expectLater(
      acquisition,
      throwsA(isA<DilockException>().having(
        (error) => error.kind,
        'kind',
        FailureKind.acquisitionUnknown,
      )),
    );
    await provider.waitForCleanup();
    expect(server.heldKeys, isEmpty);
    expect(server.ownerCleanupCommands, 1,
        reason: 'an ambiguous cleanup must not blindly retry the release RPC');
  });

  test('unknown tracked cleanup after ambiguous SET is reported to observer',
      () async {
    server.delaySetResponse = true;
    server.failNextOwnerCleanup = true;
    final token = CancellationToken();
    final handle = provider.lock('cancelled/unknown-cleanup-observed');
    final acquisition = handle.tryAcquire(cancellationToken: token);
    await Future<void>.delayed(const Duration(milliseconds: 30));
    token.cancel();

    await expectLater(
      acquisition,
      throwsA(isA<DilockException>().having(
        (error) => error.kind,
        'kind',
        FailureKind.acquisitionUnknown,
      )),
    );
    await provider.waitForCleanup();
    expect(cleanupObservations, hasLength(1));
    expect(cleanupObservations.single.outcome, ReleaseOutcome.unknown);
    expect(cleanupObservations.single.backend, 'redis');
    expect(cleanupObservations.single.reason, 'acquisition_unknown');
  });

  test('cancellation after positive SET acknowledgement stays unknown', () {
    final failure = redisCancellationAfterSetResponse(
      ownershipAcknowledged: true,
    );
    expect(failure.kind, FailureKind.acquisitionUnknown);
    expect(failure.outcomeUnknown, isTrue);
  });

  test('cancellation after confirmed SET contention stays cancelled', () {
    final failure = redisCancellationAfterSetResponse(
      ownershipAcknowledged: false,
    );
    expect(failure.kind, FailureKind.cancelled);
    expect(failure.outcomeUnknown, isFalse);
  });

  test('unsupported fencing fails before the lock command', () async {
    final handle = provider.lock('reports/no-redis-fence');
    await expectLater(
      handle.tryAcquire(options: const LeaseOptions(requireFencing: true)),
      throwsA(isA<DilockException>().having(
        (error) => error.kind,
        'kind',
        FailureKind.unsupportedCapability,
      )),
    );
    expect(server.heldKeys, isEmpty);
  });

  test('namespace metadata requires an integral protocol major', () async {
    server.setProtocolMajor(1.0);

    await expectLater(
      RedisProvider.connect(
        'redis://127.0.0.1:${server.port}/',
        options: const RedisProviderOptions(coordinationEpoch: 'test-epoch'),
      ),
      throwsA(isA<DilockException>().having(
        (error) => error.kind,
        'kind',
        FailureKind.protocolMismatch,
      )),
    );
  });

  test('AUTH and SELECT share one connection setup budget', () async {
    server.delayAuthResponse = const Duration(milliseconds: 65);
    server.delaySelectResponse = const Duration(milliseconds: 65);
    final stopwatch = Stopwatch()..start();

    await expectLater(
      RedisProvider.connect(
        'redis://user:pass@127.0.0.1:${server.port}/1',
        options: const RedisProviderOptions(
          coordinationEpoch: 'test-epoch',
          connectTimeout: Duration(milliseconds: 100),
        ),
      ),
      throwsA(isA<DilockException>().having(
        (error) => error.kind,
        'kind',
        FailureKind.backendUnavailable,
      )),
    );

    expect(stopwatch.elapsed, lessThan(const Duration(milliseconds: 220)));
  });
}

final class _FakeRedis {
  _FakeRedis._(this._server);

  final ServerSocket _server;
  final Map<String, String> _values = <String, String>{};
  final Set<Socket> _sockets = <Socket>{};
  int acceptedConnections = 0;
  int ownerCleanupCommands = 0;
  bool delaySetResponse = false;
  Completer<void>? metadataRequestStarted;
  Duration delayMetadataResponse = Duration.zero;
  Duration delayTypeResponse = Duration.zero;
  Duration delayAuthResponse = Duration.zero;
  Duration delaySelectResponse = Duration.zero;
  bool failNextOwnerCleanup = false;

  int get port => _server.port;

  void setProtocolMajor(Object? value) {
    _values['dilock:namespace:default:meta'] = jsonEncode(<String, Object?>{
      'protocol_major': value,
      'identity_scheme': 'sha256-framed-utf8-v1',
      'profile': 'redis-single-authority-v1',
      'coordination_epoch': 'test-epoch',
      'fixed_lease_ms': null,
    });
  }

  Set<String> get heldKeys =>
      _values.keys.where((key) => !key.startsWith('dilock:namespace:')).toSet();

  void clearHeldLocks() {
    for (final key in heldKeys) {
      _values.remove(key);
    }
  }

  static Future<_FakeRedis> start() async {
    final server =
        _FakeRedis._(await ServerSocket.bind(InternetAddress.loopbackIPv4, 0));
    server._values['dilock:namespace:default:meta'] =
        jsonEncode(<String, Object?>{
      'protocol_major': 1,
      'identity_scheme': 'sha256-framed-utf8-v1',
      'profile': 'redis-single-authority-v1',
      'coordination_epoch': 'test-epoch',
      'fixed_lease_ms': null,
    });
    server._server.listen(server._accept);
    return server;
  }

  void _accept(Socket socket) {
    acceptedConnections++;
    _sockets.add(socket);
    final parser = _RequestParser();
    var serial = Future<void>.value();
    socket.listen(
      (data) {
        parser.add(data);
        while (true) {
          final command = parser.nextCommand();
          if (command == null) break;
          serial = serial.then((_) => _handle(socket, command));
        }
      },
      onDone: () => _sockets.remove(socket),
      onError: (_) => _sockets.remove(socket),
      cancelOnError: true,
    );
  }

  Future<void> _handle(Socket socket, List<String> command) async {
    if (command.isEmpty) return;
    final name = command.first.toUpperCase();
    switch (name) {
      case 'PING':
        await _write(
            socket, utf8.encode('+${name == 'PING' ? 'PONG' : 'OK'}\r\n'));
        break;
      case 'AUTH':
      case 'SELECT':
        final delay = name == 'AUTH' ? delayAuthResponse : delaySelectResponse;
        if (name == 'AUTH') {
          delayAuthResponse = Duration.zero;
        } else {
          delaySelectResponse = Duration.zero;
        }
        if (delay > Duration.zero) await Future<void>.delayed(delay);
        try {
          await _write(socket, utf8.encode('+OK\r\n'));
        } on Object {
          // The client may close the socket when the shared setup budget ends.
        }
        break;
      case 'GET':
        final metadataDelay = delayMetadataResponse;
        delayMetadataResponse = Duration.zero;
        metadataRequestStarted?.complete();
        metadataRequestStarted = null;
        if (metadataDelay > Duration.zero) {
          await Future<void>.delayed(metadataDelay);
        }
        final value = _values[command[1]];
        await _write(
            socket, value == null ? utf8.encode('\$-1\r\n') : _bulk(value));
        break;
      case 'TYPE':
        final typeDelay = delayTypeResponse;
        delayTypeResponse = Duration.zero;
        if (typeDelay > Duration.zero) {
          await Future<void>.delayed(typeDelay);
        }
        await _write(
          socket,
          utf8.encode(
            '+${_values.containsKey(command[1]) ? 'string' : 'none'}\r\n',
          ),
        );
        break;
      case 'SET':
        final key = command[1];
        if (_values.containsKey(key)) {
          await _write(socket, utf8.encode('\$-1\r\n'));
        } else {
          _values[key] = command[2];
          final shouldDelayResponse = delaySetResponse;
          delaySetResponse = false;
          if (shouldDelayResponse) {
            await Future<void>.delayed(const Duration(milliseconds: 200));
          }
          try {
            await _write(socket, utf8.encode('+OK\r\n'));
          } on Object {
            // The SDK may close a cancelled command socket after dispatch.
          }
        }
        break;
      case 'EVAL':
        ownerCleanupCommands++;
        if (failNextOwnerCleanup) {
          failNextOwnerCleanup = false;
          socket.destroy();
          return;
        }
        final script = command[1];
        final key = command[3];
        final owner = command[4];
        if (_values[key] == owner) {
          if (script.contains('DEL')) {
            _values.remove(key);
          }
          await _write(socket, utf8.encode(':1\r\n'));
        } else {
          await _write(socket, utf8.encode(':0\r\n'));
        }
        break;
      default:
        await _write(socket, utf8.encode('-ERR unsupported test command\r\n'));
        break;
    }
  }

  Future<void> _write(Socket socket, List<int> response) async {
    try {
      socket.add(response);
      await socket.flush();
    } on SocketException {
      // Timed-out or cancelled client commands close their socket before a
      // delayed fake-server response is written.
    }
  }

  List<int> _bulk(String value) {
    final bytes = utf8.encode(value);
    return <int>[...utf8.encode('\$${bytes.length}\r\n'), ...bytes, 13, 10];
  }

  Future<void> close() async {
    await _server.close();
    for (final socket in List<Socket>.of(_sockets)) {
      socket.destroy();
    }
    _sockets.clear();
  }
}

final class _RequestParser {
  final List<int> _bytes = <int>[];
  int _offset = 0;

  void add(List<int> bytes) => _bytes.addAll(bytes);

  List<String>? nextCommand() {
    final start = _offset;
    final countLine = _findCrlf(start);
    if (countLine == null) return null;
    final countHeader = utf8.decode(_bytes.sublist(start, countLine));
    if (!countHeader.startsWith('*')) {
      throw const FormatException('Expected RESP array.');
    }
    final count = int.parse(countHeader.substring(1));
    var cursor = countLine + 2;
    final command = <String>[];
    for (var i = 0; i < count; i++) {
      final sizeLine = _findCrlf(cursor);
      if (sizeLine == null) return null;
      final sizeHeader = utf8.decode(_bytes.sublist(cursor, sizeLine));
      if (!sizeHeader.startsWith('\$')) {
        throw const FormatException('Expected RESP bulk argument.');
      }
      final size = int.parse(sizeHeader.substring(1));
      final dataStart = sizeLine + 2;
      final dataEnd = dataStart + size;
      if (_bytes.length < dataEnd + 2) return null;
      command.add(utf8.decode(_bytes.sublist(dataStart, dataEnd)));
      if (_bytes[dataEnd] != 13 || _bytes[dataEnd + 1] != 10) {
        throw const FormatException('Invalid RESP bulk ending.');
      }
      cursor = dataEnd + 2;
    }
    _offset = cursor;
    if (_offset > 4096) {
      _bytes.removeRange(0, _offset);
      _offset = 0;
    }
    return command;
  }

  int? _findCrlf(int start) {
    for (var i = start; i + 1 < _bytes.length; i++) {
      if (_bytes[i] == 13 && _bytes[i + 1] == 10) return i;
    }
    return null;
  }
}
