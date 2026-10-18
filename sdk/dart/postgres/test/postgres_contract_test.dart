import 'package:dilock_postgres/dilock_postgres.dart';
import 'package:test/test.dart';

void main() {
  test('advisory key uses signed big-endian digest prefix', () {
    final suffix = List<String>.filled(48, '0').join();
    expect(advisoryKey('0000000000000000$suffix'), BigInt.zero);
    expect(advisoryKey('7fffffffffffffff$suffix'), (BigInt.one << 63) - BigInt.one);
    expect(advisoryKey('8000000000000000$suffix'), -(BigInt.one << 63));
    expect(advisoryKey('ffffffffffffffff$suffix'), BigInt.from(-1));
  });
}
