-- Run this handler with an ngx_lua "dilock" thread pool. The required provider
-- is configured once per worker in billing/dilock_provider.lua and reused.
local provider = require "billing.dilock_provider"
local result, operation_err = provider:with_lock("invoice:42", function(lease)
    -- Persist lease.fence.create_revision with the protected resource and have
    -- that resource reject stale fencing revisions.
    ngx.sleep(0.01)
    return lease.fence.create_revision
end)
if not result then
    ngx.status = operation_err == "wait_timeout" and ngx.HTTP_CONFLICT or ngx.HTTP_SERVICE_UNAVAILABLE
    ngx.say(operation_err)
    return
end
ngx.say("completed with fencing revision ", result)
