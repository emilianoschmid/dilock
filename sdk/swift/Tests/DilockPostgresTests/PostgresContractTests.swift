import Testing
@testable import DilockPostgres

@Test("PostgreSQL advisory key preserves signed int64 bit pattern")
func advisoryKeyVector() {
    #expect(PostgresProvider.advisoryKey("0000000000000000" + String(repeating: "0", count: 48)) == 0)
    #expect(PostgresProvider.advisoryKey("ffffffffffffffff" + String(repeating: "0", count: 48)) == -1)
    #expect(PostgresProvider.advisoryKey("8000000000000000" + String(repeating: "0", count: 48)) == Int64.min)
}
