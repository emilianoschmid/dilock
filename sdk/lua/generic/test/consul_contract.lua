local consul = require "dilock.consul"
local function check(value, message) if not value then error(message, 2) end end

check(consul._decimal_index('[{"LockIndex":18446744073709551615}]') == "18446744073709551615",
    "Consul LockIndex must remain an exact decimal string")
check(consul._decimal_index('[{"LockIndex":1,"LockIndex":2}]') == nil,
    "ambiguous duplicate LockIndex must fail closed")
local provider, status = consul.new({ epoch = "epoch-1", endpoint = "https://consul.example" })
check(not provider and status == "unsupported_endpoint", "LuaSocket SDK must reject HTTPS without TLS transport")
provider, status = consul.new({ epoch = "epoch-1", token = "bad\nheader" })
check(not provider and status == "invalid_token", "ACL token must be header safe")
local provisioned, provision_status = consul.provision({ epoch = "epoch-1", endpoint = "https://consul.example" })
check(not provisioned and provision_status == "unsupported_endpoint", "provisioning must enforce the same transport boundary")

local json = require "dkjson"
local http = require "socket.http"
local original_request = http.request
local owner_session, lock_index, session_sequence = nil, 0, 0
local malformed_owner_read, destroy_calls, lock_path = false, 0, nil
local metadata_value = "eyJwcm90b2NvbF9tYWpvciI6MSwiaWRlbnRpdHlfc2NoZW1lIjoic2hhMjU2LWZyYW1lZC11dGY4LXYxIiwicHJvZmlsZSI6ImNvbnN1bC1rdi1zZXNzaW9uLXYxIiwiY29vcmRpbmF0aW9uX2Vwb2NoIjoiZXBvY2giLCJmaXhlZF9sZWFzZV9tcyI6bnVsbH0="
http.request = function(request)
    local path = request.url:match("^http://[^/]+(.*)$")
    local response
    if request.method == "PUT" and path:find("/v1/kv/dilock/namespaces/default/meta?cas=0", 1, true) then
        response = "true"
    elseif request.method == "GET" and path:find("/v1/kv/dilock/namespaces/default/meta?", 1, true) then
        response = json.encode({ { Key = "dilock/namespaces/default/meta", Value = metadata_value } })
    elseif request.method == "PUT" and path == "/v1/session/create" then
        session_sequence = session_sequence + 1
        response = json.encode({ ID = "session-" .. session_sequence })
    elseif request.method == "PUT" and path:find("?acquire=", 1, true) then
        local query = path:match("%?acquire=(.+)$")
        lock_path = path:sub(1, path:find("?acquire=", 1, true) - 1)
        owner_session = query
        lock_index = lock_index + 1
        response = "true"
    elseif request.method == "GET" and lock_path and path:sub(1, #lock_path) == lock_path then
        if malformed_owner_read then
            malformed_owner_read = false
            response = "not-json"
        else
            response = json.encode({ { Key = lock_path:sub(#"/v1/kv/" + 1), Session = owner_session, LockIndex = lock_index } })
        end
    elseif request.method == "PUT" and path:find("/v1/session/destroy/", 1, true) then
        destroy_calls = destroy_calls + 1
        response = "true"
    else
        error("unexpected mocked Consul request: " .. tostring(path))
    end
    request.sink(response)
    return 1, 200, {}, "OK"
end

local function check_ambiguous_release_still_destroys_session()
    local ok, failure = pcall(function()
        local configured = assert(consul.provision({ endpoint = "http://consul.example", epoch = "epoch" }))
        check(configured, "namespace provisioning should be acknowledged")
        local provider = assert(consul.new({ endpoint = "http://consul.example", epoch = "epoch",
            rpc_timeout_ms = 1000, cleanup_timeout_ms = 1000, default_lease_ms = 10000 }))
        local handle = assert(provider:lock("release-read"))
        local lease = assert(handle:try_acquire(10000))
        malformed_owner_read = true
        local previous_destroys = destroy_calls
        check(lease:release() == "unknown", "an ambiguous owner read must not mean already-not-owned")
        check(destroy_calls == previous_destroys + 1, "ambiguous release must still destroy its session")
        local closed, close_status = provider:close()
        check(not closed and close_status == "release_unknown", "provider close must preserve unknown release")
    end)
    http.request = original_request
    if not ok then error(failure, 0) end
end
check_ambiguous_release_still_destroys_session()
print("Lua 5.1 Consul contract: PASS")
