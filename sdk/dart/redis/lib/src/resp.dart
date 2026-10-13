part of 'provider.dart';

/// One private RESP2 connection borrowed exclusively from a provider pool.
final class _RedisConnection {
  _RedisConnection(this._socket) : _reader = _RespReader(_socket);

  final Socket _socket;
  final _RespReader _reader;
  bool _closed = false;

  bool get isOpen => !_closed;

  Future<Object?> execute(List<Object?> args) async {
    if (_closed) throw const SocketException('Redis connection is closed.');
    final bytes = _encodeCommand(args);
    var dispatched = false;
    try {
      _socket.add(bytes);
      dispatched = true;
      await _socket.flush();
      return await _reader.readValue();
    } on RedisServerException {
      rethrow;
    } on Object catch (error) {
      throw _RedisTransportException(dispatched, error);
    }
  }

  void close() {
    if (_closed) return;
    _closed = true;
    _reader.close();
    _socket.destroy();
  }
}

Uint8List _encodeCommand(List<Object?> args) {
  final builder = BytesBuilder(copy: false)
    ..add(ascii.encode('*${args.length}\r\n'));
  for (final value in args) {
    if (value == null) {
      throw ArgumentError.value(
        value,
        'args',
        'RESP command arguments cannot be null.',
      );
    }
    final bytes = value is List<int> ? value : utf8.encode(value.toString());
    builder
      ..add(ascii.encode('\$${bytes.length}\r\n'))
      ..add(bytes)
      ..add(ascii.encode('\r\n'));
  }
  return builder.takeBytes();
}

final class _RespReader {
  _RespReader(Stream<List<int>> stream)
      : _chunks = StreamIterator<List<int>>(stream);

  final StreamIterator<List<int>> _chunks;
  List<int> _current = const <int>[];
  int _offset = 0;
  bool _closed = false;

  Future<Object?> readValue() async {
    final marker = await _byte();
    switch (marker) {
      case 43: // + simple string
        return utf8.decode(await _line());
      case 45: // - error
        throw RedisServerException(
          utf8.decode(await _line(), allowMalformed: true),
        );
      case 58: // : integer
        return int.parse(ascii.decode(await _line()));
      case 36: // $ bulk string
        final size = int.parse(ascii.decode(await _line()));
        if (size < 0) return null;
        final value = await _exact(size);
        await _expectCrlf();
        return value;
      case 42: // * array
        final count = int.parse(ascii.decode(await _line()));
        if (count < 0) return null;
        final values = <Object?>[];
        for (var i = 0; i < count; i++) {
          values.add(await readValue());
        }
        return values;
      default:
        throw const FormatException('Invalid RESP2 value marker.');
    }
  }

  Future<int> _byte() async {
    while (_offset >= _current.length) {
      if (_closed || !await _chunks.moveNext()) {
        throw const SocketException(
          'Redis closed its connection before the response arrived.',
        );
      }
      _current = _chunks.current;
      _offset = 0;
    }
    return _current[_offset++];
  }

  Future<List<int>> _line() async {
    final bytes = <int>[];
    while (true) {
      final byte = await _byte();
      if (byte == 13) {
        final lf = await _byte();
        if (lf != 10) throw const FormatException('Invalid RESP2 line ending.');
        return bytes;
      }
      bytes.add(byte);
    }
  }

  Future<List<int>> _exact(int length) async {
    final result = List<int>.filled(length, 0);
    for (var i = 0; i < length; i++) {
      result[i] = await _byte();
    }
    return result;
  }

  Future<void> _expectCrlf() async {
    if (await _byte() != 13 || await _byte() != 10) {
      throw const FormatException('Invalid RESP2 bulk string ending.');
    }
  }

  void close() {
    _closed = true;
    unawaited(_chunks.cancel());
  }
}

final class _RedisTransportException implements Exception {
  const _RedisTransportException(this.dispatched, this.cause);

  final bool dispatched;
  final Object cause;
}
