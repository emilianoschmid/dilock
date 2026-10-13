#include "dilock/redis.hpp"
#include "metadata.hpp"
#include "redis_scripts.hpp"

#include <nlohmann/json.hpp>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace {
void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}
}

int main() {
    try {
        const dilock::RedisOptions defaults;
        require(defaults.default_acquisition.wait == std::chrono::seconds(5),
                "default acquisition wait must be five seconds");
        require(defaults.default_acquisition.lease == std::chrono::seconds(30),
                "default lease TTL must be thirty seconds");

        require(dilock::detail::has_protocol_major_v1(nlohmann::json::parse(R"({"protocol_major":1})")),
                "integer protocol_major 1 must be accepted");
        for (const auto* malformed : {
                 R"({"protocol_major":1.0})",
                 R"({"protocol_major":1.5})",
                 R"({"protocol_major":"1"})",
                 R"({"protocol_major":true})",
                 R"({"protocol_major":4294967297})",
                 R"({})",
                 R"([])"}) {
            require(!dilock::detail::has_protocol_major_v1(nlohmann::json::parse(malformed)),
                    std::string("malformed protocol_major was accepted: ") + malformed);
        }
        for (const auto* ambiguous : {
                 R"({"protocol_major":1,"protocol_major":1})",
                 R"({"coordination_epoch":"old","coordination_epoch":"new"})",
                 R"({"protocol_major":1,"protocol\u005fmajor":1})"}) {
            bool rejected = false;
            try {
                (void)dilock::detail::parse_metadata(ambiguous);
            } catch (const std::exception&) {
                rejected = true;
            }
            require(rejected, std::string("duplicate metadata property was accepted: ") + ambiguous);
        }
        (void)dilock::detail::parse_metadata(
            R"({"protocol_major":1,"extension":{"key":1,"key":2}})");
        using DefaultAcquire = dilock::Lease (dilock::ResourceLock::*)(std::stop_token) const;
        using PolicyAcquire = dilock::Lease (dilock::ResourceLock::*)(const dilock::AcquireOptions&, std::stop_token) const;
        static_assert(std::is_same_v<decltype(static_cast<DefaultAcquire>(&dilock::ResourceLock::acquire)), DefaultAcquire>);
        static_assert(std::is_same_v<decltype(static_cast<PolicyAcquire>(&dilock::ResourceLock::acquire)), PolicyAcquire>);

        std::ifstream input(DILOCK_IDENTITY_VECTORS);
        require(static_cast<bool>(input), "identity vector file could not be opened");
        std::string vector_text((std::istreambuf_iterator<char>(input)), {});
        // The shared invalid-vector set includes an escaped lone UTF-16
        // surrogate. nlohmann::json correctly rejects that text before the
        // SDK can receive it, so replace it only for parsing and test the
        // equivalent ill-formed UTF-8 byte sequence below.
        const std::string lone_surrogate = R"(\ud800)";
        const auto surrogate_pos = vector_text.find(lone_surrogate);
        require(surrogate_pos != std::string::npos, "lone-surrogate vector is missing");
        vector_text.replace(surrogate_pos, lone_surrogate.size(), R"(\ufffd)");
        const auto vectors = nlohmann::json::parse(vector_text);
        for (const auto& vector : vectors.at("valid")) {
            const auto id = dilock::resource_id(vector.at("namespace").get<std::string>(),
                                                vector.at("resource").get<std::string>());
            require(id == vector.at("sha256").get<std::string>(),
                    "identity mismatch for " + vector.at("id").get<std::string>());
            require("dilock:{" + id + "}:lease" == vector.at("redis_key").get<std::string>(),
                    "Redis key mismatch for " + vector.at("id").get<std::string>());
        }
        for (const auto& vector : vectors.at("invalid")) {
            if (vector.at("id").get<std::string>() == "surrogate-resource") {
                const std::string invalid_utf8("\xed\xa0\x80", 3);
                bool rejected = false;
                try {
                    (void)dilock::resource_id("default", invalid_utf8);
                } catch (const dilock::Error&) {
                    rejected = true;
                }
                require(rejected, "UTF-8 encoded surrogate was accepted");
                continue;
            }
            bool rejected = false;
            try {
                (void)dilock::resource_id(vector.at("namespace").get<std::string>(),
                                          vector.at("resource").get<std::string>());
            } catch (const dilock::Error&) {
                rejected = true;
            } catch (const nlohmann::json::type_error&) {
                rejected = true;
            }
            require(rejected, "invalid identity input was accepted for " + vector.at("id").get<std::string>());
        }

        for (const auto& scalar : {std::string("\xf1\x80\x80\x80", 4),
                                   std::string("\xf3\xbf\xbf\xbf", 4),
                                   std::string("\xf4\x8f\xbf\xbf", 4)}) {
            require(dilock::resource_id("default", scalar).size() == 64,
                    "valid four-byte Unicode scalar was rejected");
        }
        for (const auto& malformed : {std::string("\xf1\x7f\x80\x80", 4),
                                      std::string("\xf3\xbf\xbf\xc0", 4),
                                      std::string("\xf4\x90\x80\x80", 4),
                                      std::string("\xf0\x8f\xbf\xbf", 4),
                                      std::string("\xf5\x80\x80\x80", 4),
                                      std::string("\xf1\x80\x80", 3)}) {
            bool rejected = false;
            try { (void)dilock::resource_id("default", malformed); }
            catch (const dilock::Error&) { rejected = true; }
            require(rejected, "malformed UTF-8 byte sequence was accepted");
        }

        std::ifstream canonical_release(DILOCK_RELEASE_SOURCE, std::ios::binary);
        std::ifstream canonical_renew(DILOCK_RENEW_SOURCE, std::ios::binary);
        require(static_cast<bool>(canonical_release) && static_cast<bool>(canonical_renew),
                "canonical Redis scripts could not be opened");
        const std::string release((std::istreambuf_iterator<char>(canonical_release)), {});
        const std::string renew((std::istreambuf_iterator<char>(canonical_renew)), {});
        require(release == dilock::detail::release_script, "embedded release script differs from spec source");
        require(renew == dilock::detail::renew_script, "embedded renew script differs from spec source");
        std::cout << "PASS: identity vectors, metadata scalar types and canonical Redis scripts\n";
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
