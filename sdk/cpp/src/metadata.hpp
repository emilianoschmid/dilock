#pragma once

#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>

namespace dilock::detail {

using Json = nlohmann::json;

// Parse namespace metadata while preserving the distinction between unique
// protocol fields and ambiguous duplicate top-level names. Nested unknown
// extension objects are intentionally left opaque.
inline Json parse_metadata(std::string_view raw) {
    std::unordered_set<std::string> top_level_keys;
    const auto callback = [&top_level_keys](int depth, Json::parse_event_t event, Json& parsed) {
        if (depth == 1 && event == Json::parse_event_t::key &&
            !top_level_keys.insert(parsed.get<std::string>()).second) {
            throw std::invalid_argument("duplicate top-level metadata property");
        }
        return true;
    };
    return Json::parse(raw.begin(), raw.end(), callback);
}

// Keep protocol version parsing type-strict across SDKs. JSON permits a
// fractional number with an integral value (1.0), but the shared metadata
// contract uses an integer protocol major and the other SDKs reject that
// representation rather than coercing it.
inline bool has_protocol_major_v1(const nlohmann::json& value) noexcept {
    if (!value.is_object() || !value.contains("protocol_major")) return false;
    const auto& major = value["protocol_major"];
    return major.is_number_integer() && major == 1;
}

} // namespace dilock::detail
