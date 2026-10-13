-- Run only from an OpenResty content_by_lua* request handler.
local dilock = require "dilock"
local provider, err = dilock.new({
    host = "127.0.0.1", port = 6379,
    namespace = "billing", epoch = "prod-east-1",
})
assert(provider, err)
local handle
handle, err = provider:lock("invoice:42")
assert(handle, err)
local value, scope_err, release_outcome, release_err = handle:with_lock(function(lease)
    -- Keep protected work within the lease; expiry does not stop stale work.
    assert(lease.owner, "the active lease is available to the callback")
    return "invoice:42 updated"
end, 30000)
if scope_err == "busy" then
    ngx.status = ngx.HTTP_CONFLICT
    ngx.say("resource busy")
    return
end
if scope_err == "release_unknown" then
    ngx.log(ngx.ERR, "Dilock release outcome unknown: ", release_err or "")
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.say("lock release could not be confirmed")
    return
end
if scope_err == "lease_lost" then
    ngx.log(ngx.ERR, "Dilock lease lost: ", release_outcome or "")
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.say("lock lease was lost during the operation")
    return
end
if scope_err then
    ngx.log(ngx.ERR, "Dilock operation failed: ", scope_err)
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.say("lock operation failed")
    return
end
ngx.say(value)
