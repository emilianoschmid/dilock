-- Deterministic contract and lifecycle tests for the OpenResty Consul facade.
local cjson = require "cjson.safe"
local clock, session_sequence, owner_session, lock_index = 10, 0, nil, 0
local malformed_owner_read, destroy_calls = false, 0
local requests, tls_options = {}, nil
ngx = { get_phase = function() return "content" end,
    sleep = function(seconds) clock = clock + seconds end, socket = { tcp = function() end } }
package.loaded["resty.core.time"] = { monotonic_time = function() return clock end }
package.preload["dilock"] = function()
    return { resource_id = function(namespace, resource)
        if type(namespace) ~= "string" or not namespace:match("^[a-z0-9][a-z0-9_.%-]*$") then return nil, "invalid_namespace" end
        if type(resource) ~= "string" or resource == "" or #resource > 1024 or resource:find("%z") then return nil, "invalid_resource" end
        return string.rep("a", 64)
    end }
end
package.preload["resty.http"] = function()
    return { new = function()
        local client = {}
        function client:set_timeouts(connect, send, read)
            assert(connect > 0 and send > 0 and read > 0)
        end
        function client:request_uri(uri, options)
            requests[#requests + 1] = { uri = uri, options = options }
            tls_options = options
            if uri:match("/v1/kv/dilock/namespaces/") and options.method == "PUT" then
                assert(uri:find("cas=0", 1, true), "provisioning must use CAS=0")
                return { status = 200, body = "true" }
            elseif uri:match("/v1/kv/dilock/namespaces/") then
                return { status = 200, body = cjson.encode({ {
                    Key = "dilock/namespaces/default/meta",
                    Value = "eyJwcm90b2NvbF9tYWpvciI6MSwiaWRlbnRpdHlfc2NoZW1lIjoic2hhMjU2LWZyYW1lZC11dGY4LXYxIiwicHJvZmlsZSI6ImNvbnN1bC1rdi1zZXNzaW9uLXYxIiwiY29vcmRpbmF0aW9uX2Vwb2NoIjoiZXBvY2giLCJmaXhlZF9sZWFzZV9tcyI6bnVsbH0=",
                } }) }
            elseif uri:match("/v1/session/create") then
                session_sequence = session_sequence + 1
                return { status = 200, body = cjson.encode({ ID = "session-" .. session_sequence }) }
            elseif uri:match("/v1/session/renew/") then
                local id = uri:match("/renew/([^?]+)")
                return { status = 200, body = cjson.encode({ { ID = id } }) }
            elseif uri:match("/v1/session/destroy/") then
                destroy_calls = destroy_calls + 1
                return { status = 200, body = "true" }
            elseif uri:match("/v1/kv/") and options.method == "PUT" and uri:find("acquire=", 1, true) then
                local id = uri:match("acquire=([^&]+)")
                if owner_session then return { status = 200, body = "false" } end
                owner_session = id
                lock_index = lock_index + 1
                return { status = 200, body = "true" }
            elseif uri:match("/v1/kv/") and options.method == "GET" then
                if malformed_owner_read then malformed_owner_read = false; return { status = 200, body = "not-json" } end
                if not owner_session then return { status = 404, body = "" } end
                return { status = 200, body = cjson.encode({ {
                    Key = uri:match("/v1/kv/([^?]+)"), Session = owner_session, LockIndex = lock_index,
                } }) }
            elseif uri:match("/v1/kv/") and options.method == "PUT" and uri:find("release=", 1, true) then
                local id = uri:match("release=([^&]+)")
                if id == owner_session then owner_session = nil; return { status = 200, body = "true" } end
                return { status = 200, body = "false" }
            end
            error("unexpected Consul request: " .. uri)
        end
        return client
    end }
end
package.path = "sdk/lua/lib/?.lua;" .. package.path
local consul = require "dilock_consul"
local invalid, err = consul.new({ epoch = "epoch", verify_tls = "true" })
assert(not invalid and err == "invalid_verify_tls")
invalid, err = consul.new({ epoch = "epoch", endpoint = "https://host/path" })
assert(not invalid and err == "invalid_endpoint")
assert(consul.provision({ endpoint = "https://consul.internal:8501", epoch = "epoch" }))
local provider = assert(consul.new({ endpoint = "https://consul.internal:8501", epoch = "epoch",
    rpc_timeout_ms = 1500, cleanup_timeout_ms = 1000, default_lease_ms = 10000 }))
assert(tls_options.ssl_verify == true and tls_options.ssl_server_name == nil,
    "TLS verification must default on and SNI must follow the Consul host")
local handle = assert(provider:lock("resource"))
local first = assert(handle:acquire({ wait_ms = 500, lease_ms = 10000 }))
assert(first.fence.coordination_epoch == "epoch" and first.fence.lock_index == "1")
local busy, busy_status = handle:try_acquire(10000)
assert(not busy and busy_status == "busy", tostring(busy_status))
assert(first:renew(), "renewal must confirm the same session and LockIndex")
assert(first:release() == "released")
local second = assert(handle:acquire({ wait_ms = 500, lease_ms = 10000 }))
assert(second.fence.lock_index == "2", "successor receives a higher exact LockIndex")
assert(second:release() == "released")
local third = assert(handle:acquire({ wait_ms = 500, lease_ms = 10000 }))
malformed_owner_read = true
local before_destroy = destroy_calls
assert(third:release() == "unknown", "an unreadable owner must not be reported as not-owned")
assert(destroy_calls == before_destroy + 1, "an ambiguous release must still attempt bounded session destruction")
local closed, close_status = provider:close()
assert(not closed and close_status == "release_unknown", "provider close must preserve the uncertain release outcome")
print("OpenResty Consul deterministic contract: PASS")
