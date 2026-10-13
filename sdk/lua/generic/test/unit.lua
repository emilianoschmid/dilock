local dilock = require "dilock"

local function expect(value, label)
    assert(value, label)
end

local vector_path = assert(os.getenv("DILOCK_VECTOR_PATH"), "DILOCK_VECTOR_PATH is required")
local vector_file = assert(io.open(vector_path, "rb"))
local vector_source = vector_file:read("*a")
vector_file:close()
local vectors, _, vector_err = require("dkjson").decode(vector_source)
expect(vectors ~= nil and not vector_err, "identity vectors JSON failed to decode: " .. tostring(vector_err))
for _, vector in ipairs(vectors.valid) do
    local actual, err = dilock.resource_id(vector.namespace, vector.resource)
    expect(actual == vector.sha256, "identity vector mismatch " .. vector.id .. ": " .. tostring(err))
    expect("dilock:{" .. actual .. "}:lease" == vector.redis_key, "Redis key vector mismatch " .. vector.id)
end

local _, err = dilock.resource_id("Invalid", "r")
expect(err == "invalid_namespace", "invalid namespace must be rejected")
_, err = dilock.resource_id("default", "")
expect(err == "invalid_resource", "empty resource must be rejected")
_, err = dilock.resource_id("default", "a\0b")
expect(err == "invalid_resource", "NUL resource must be rejected")
_, err = dilock.resource_id("default", string.char(0xc0, 0xaf))
expect(err == "invalid_resource", "overlong UTF-8 must be rejected")
_, err = dilock.resource_id("default", string.char(0xf1, 0x41, 0x80, 0x80))
expect(err == "invalid_resource", "non-continuation second byte in four-byte UTF-8 must be rejected")
_, err = dilock.resource_id("default", string.char(0xf3, 0xc0, 0x80, 0x80))
expect(err == "invalid_resource", "out-of-range second byte in four-byte UTF-8 must be rejected")

local provider
provider, err = dilock.new({ epoch = "deployment-1", tls = true })
expect(provider == nil and err == "unsupported", "TLS must fail explicitly")
provider, err = dilock.new({ epoch = "deployment-1", username = "user" })
expect(provider == nil and err == "invalid_config", "username without password must fail")
provider, err = dilock.new({ epoch = "deployment-1" })
expect(provider ~= nil and err == nil, "valid provider configuration rejected")
local handle
handle, err = provider:lock("resource")
expect(handle ~= nil and err == nil, "valid resource handle rejected")
local invalid
invalid, err = provider:lock("\255")
expect(invalid == nil and err == "invalid_resource", "invalid UTF-8 handle must be rejected")

print("Lua generic unit: PASS")
