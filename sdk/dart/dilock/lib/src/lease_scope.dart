/// Runs scoped work and invokes cleanup once on either exit path.
///
/// If [work] fails, a failure from [cleanup] cannot replace the original
/// failure. On normal completion, cleanup failures remain visible to the
/// caller. Cleanup must be bounded by the lease adapter.
Future<T> runLeaseScope<T>({
  required Future<T> Function() work,
  required Future<void> Function({required bool bodyFailed}) cleanup,
}) async {
  late final T result;
  try {
    result = await work();
  } on Object catch (error, stackTrace) {
    try {
      await cleanup(bodyFailed: true);
    } on Object {
      // Keep the operation failure primary when cleanup also fails.
    }
    Error.throwWithStackTrace(error, stackTrace);
  }
  await cleanup(bodyFailed: false);
  return result;
}
