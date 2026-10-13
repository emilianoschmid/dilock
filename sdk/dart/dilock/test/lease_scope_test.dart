import 'package:dilock/src/lease_scope.dart';
import 'package:test/test.dart';

void main() {
  test('cleanup failure does not replace a work failure', () async {
    final workFailure = StateError('work failed');
    final cleanupFailure = StateError('cleanup failed');
    var observedBodyFailure = false;

    await expectLater(
      runLeaseScope<void>(
        work: () async => throw workFailure,
        cleanup: ({required bool bodyFailed}) async {
          observedBodyFailure = bodyFailed;
          throw cleanupFailure;
        },
      ),
      throwsA(same(workFailure)),
    );
    expect(observedBodyFailure, isTrue);
  });

  test('cleanup failure is visible after successful work', () async {
    final cleanupFailure = StateError('cleanup failed');
    var observedBodyFailure = true;

    await expectLater(
      runLeaseScope<int>(
        work: () async => 7,
        cleanup: ({required bool bodyFailed}) async {
          observedBodyFailure = bodyFailed;
          throw cleanupFailure;
        },
      ),
      throwsA(same(cleanupFailure)),
    );
    expect(observedBodyFailure, isFalse);
  });

  test('successful scoped work returns after cleanup', () async {
    var cleanupCalls = 0;
    final result = await runLeaseScope<int>(
      work: () async => 7,
      cleanup: ({required bool bodyFailed}) async {
        expect(bodyFailed, isFalse);
        cleanupCalls++;
      },
    );

    expect(result, 7);
    expect(cleanupCalls, 1);
  });
}
