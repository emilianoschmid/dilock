#include <dilock/etcd.hpp>
#include <dilock/postgres.hpp>
#include <dilock/redis.hpp>

#include <iostream>
#include <stdexcept>
#include <string>

int main() {
    try {
        // Exercise the public option types without requiring Redis, etcd or
        // PostgreSQL services in an artifact-only package consumer.
        const dilock::RedisOptions redis_options{};
        const dilock::EtcdOptions etcd_options{};
        const dilock::PostgresOptions postgres_options{};
        if (redis_options.name_space.empty() || etcd_options.name_space.empty() || postgres_options.name_space.empty()) {
            throw std::runtime_error("Dilock package consumer options were not initialized");
        }

        const auto postgres_key = dilock::postgres_advisory_key(
            "e0cdd484a99b6aee4162335939d2a2d128a490c097069fd85b2fc1e540a940b6");
        if (postgres_key == 0) {
            throw std::runtime_error("PostgreSQL package consumer returned an invalid advisory key");
        }

        std::cout << "Exact installed Dilock::redis, Dilock::etcd and Dilock::postgres package consumer passed.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
