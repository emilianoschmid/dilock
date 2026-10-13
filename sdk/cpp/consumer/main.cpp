#include <dilock/redis.hpp>

int main() {
    dilock::RedisOptions options;
    options.name_space = "consumer-smoke";
    return options.name_space.empty() ? 1 : 0;
}
