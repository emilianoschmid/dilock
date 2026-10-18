#include "dilock/postgres.hpp"

#include <cstdint>
#include <limits>
#include <string>

#define CHECK(condition) do { if (!(condition)) return __LINE__; } while (false)

int main() {
    const std::string zero(64, '0');
    const std::string max = "7fffffffffffffff" + std::string(48, '0');
    const std::string min = "8000000000000000" + std::string(48, '0');
    const std::string neg = "ffffffffffffffff" + std::string(48, '0');
    CHECK(dilock::postgres_advisory_key(zero) == 0);
    CHECK(dilock::postgres_advisory_key(max) == std::numeric_limits<std::int64_t>::max());
    CHECK(dilock::postgres_advisory_key(min) == std::numeric_limits<std::int64_t>::min());
    CHECK(dilock::postgres_advisory_key(neg) == -1);
    return 0;
}
