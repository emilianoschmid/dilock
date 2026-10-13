import 'package:dilock_conformance_worker/src/protocol.dart';
import 'package:test/test.dart';

void main() {
  group('JSONL request protocol', () {
    test('accepts valid acquisition and renewal request envelopes', () {
      expect(
        parseRequest(
          '{"id":"a1","op":"acquire","resource":"orders",'
          '"wait_ms":500,"lease_ms":3000,"auto_renew":true}',
        )['resource'],
        'orders',
      );
      expect(
        parseRequest(
          '{"id":"r1","op":"renew","lease_id":"opaque",'
          '"lease_ms":3000}',
        )['lease_ms'],
        3000,
      );
    });

    test(
      'rejects unknown fields, operations, invalid bounds and missing data',
      () {
        for (final invalid in <String>[
          '{"id":"a","op":"acquire","resource":"x","owner":"secret"}',
          '{"id":"a","op":"something"}',
          '{"id":"a","op":"acquire","resource":"x","wait_ms":-1}',
          '{"id":"a","op":"acquire","resource":"x","lease_ms":0}',
          '{"id":"a","op":"release"}',
          '{"id":"","op":"hello"}',
          '{"id":"a","op":"acquire","resource":"x", "auto_renew":1}',
        ]) {
          expect(() => parseRequest(invalid), throwsFormatException);
        }
      },
    );

    test('enforces id length and line byte limit constants', () {
      final oversizedId = 'x' * (maxRequestIdLength + 1);
      expect(
        () => parseRequest('{"id":"$oversizedId","op":"hello"}'),
        throwsFormatException,
      );
      expect(maxLineBytes, 65536);
    });
  });
}
