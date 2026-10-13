import 'dart:convert';

import 'package:dilock/dilock.dart';
import 'package:dilock/src/metadata_contract.dart';
import 'package:test/test.dart';

void main() {
  const profiles = <String>[
    'redis-single-authority-v1',
    'etcd-native-lock-v1',
  ];

  Map<String, dynamic> metadataWith(Object? protocolMajor, String profile) =>
      jsonDecode(jsonEncode(<String, Object?>{
        'protocol_major': protocolMajor,
        'identity_scheme': 'sha256-framed-utf8-v1',
        'profile': profile,
        'coordination_epoch': 'test-epoch',
        'fixed_lease_ms': null,
      })) as Map<String, dynamic>;

  for (final profile in profiles) {
    test('$profile accepts integer protocol major 1', () {
      expect(
        () => validateNamespaceMetadata(
          metadataWith(1, profile),
          profile: profile,
          coordinationEpoch: 'test-epoch',
        ),
        returnsNormally,
      );
    });

    test('$profile rejects fractional protocol major 1.0', () {
      expect(
        () => validateNamespaceMetadata(
          metadataWith(1.0, profile),
          profile: profile,
          coordinationEpoch: 'test-epoch',
        ),
        throwsA(isA<DilockException>().having(
          (error) => error.kind,
          'kind',
          FailureKind.protocolMismatch,
        )),
      );
    });
  }
}
