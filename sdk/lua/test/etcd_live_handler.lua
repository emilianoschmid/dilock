local dilock = require "dilock_etcd"
local provider, err = dilock.new({
    endpoint = "etcd:2379", namespace = "lua-thread-probe",
    epoch = "lua-thread-probe-v1", rpc_timeout_ms = 500,
    cleanup_timeout_ms = 1000, safety_margin_ms = 50,
    default_wait_ms = 6000, default_lease_ms = 1500,
    auto_renew = true, thread_pool = "dilock",
})
if not provider then ngx.status = 500; ngx.say(err); return end
local resource = ngx.var.arg_resource or "lua-run-worker/probe"
local wait_ms, lease_ms = tonumber(ngx.var.arg_wait) or 6000, tonumber(ngx.var.arg_lease) or 1500
if ngx.var.arg_op == "queued" then
    local lock = assert(provider:lock(resource))
    local lease, acquire_err = lock:acquire({ wait_ms = wait_ms, lease_ms = lease_ms })
    ngx.log(ngx.NOTICE, "dilock queued test acquire returned: ", lease and "acquired" or tostring(acquire_err))
    ngx.status = lease and 200 or 409
    ngx.say("outcome=", lease and "acquired" or acquire_err)
    if lease then lease:release() end
    return
end
local result, operation_err = provider:with_lock(resource,
    { wait_ms = wait_ms, lease_ms = lease_ms }, function(lease)
        local before = lease:is_valid()
        ngx.log(ngx.NOTICE, "dilock scope acquired fence=", lease.fence.create_revision)
        ngx.sleep((tonumber(ngx.var.arg_hold) or 0) / 1000)
        local after = lease:is_valid()
        ngx.log(ngx.NOTICE, "dilock scope callback ended abort_requested=", tostring(lease:abort_requested()))
        return table.concat({ "revision=", lease.fence.create_revision,
            "epoch=", lease.fence.coordination_epoch, "ttl_ms=", lease.effective_ttl_ms,
            "valid_before=", tostring(before), "valid_after=", tostring(after) }, " ")
    end)
if not result then
    ngx.log(ngx.NOTICE, "dilock etcd acquire ended: ", tostring(operation_err))
    ngx.status = operation_err == "cancelled" and 409 or 500
    ngx.say("error=", operation_err)
    return
end
ngx.say(result)
