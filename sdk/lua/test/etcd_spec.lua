-- OpenResty facade regression tests; no etcd connection is made.
local ffi = require "ffi"
local callbacks, calls, trigger_abort, release_count, fail_release_worker = {}, {}, false, 0, false
local yield_release = false
local native_release_outcome = 0
local cancellation_requests, cancellation_destroys = 0, 0
local state = { ctx = {} }
package.preload["ngx.semaphore"] = function()
    return { new = function()
        local sem = { held = false }
        function sem:wait() assert(not self.held); self.held = true; return true end
        function sem:post() assert(self.held); self.held = false; return true end
        return sem
    end }
end
ngx = {
    ctx = state.ctx,
    on_abort = function(callback) callbacks[#callbacks + 1] = callback; return true end,
    run_worker_thread = function(pool, module, function_name, ...)
        assert(pool == "dilock" and module == "dilock_etcd_worker")
        local args = { ... }
        for _, value in ipairs(args) do
            assert(type(value) ~= "cdata", "worker arguments must never contain cdata")
        end
        calls[#calls + 1] = { name = function_name, args = args }
        if function_name == "abi_version" then return true, 1 end
        if function_name == "create_cancellation" then return true, 0, "", 13, 14 end
        if function_name == "request_cancellation" then
            assert(args[1] == 13 and args[2] == 14, "cancellation pointer pair stays exact")
            cancellation_requests = cancellation_requests + 1
            return true, true
        end
        if function_name == "destroy_cancellation" then
            assert(args[1] == 13 and args[2] == 14, "destroy uses the same cancellation token")
            cancellation_destroys = cancellation_destroys + 1
            return true, true
        end
        if function_name == "lease_valid" then return true, true end
        if function_name == "create_provider" then return true, 0, "", 11, 12 end
        if function_name == "create_lock" then return true, 0, "", 3, 4 end
        if function_name == "acquire" then
            assert(args[1] == 11 and args[2] == 12 and args[3] == 3 and args[4] == 4)
            assert(type(args[5]) == "number" and type(args[6]) == "number", "cancellation pointer is split")
            if trigger_abort then
                trigger_abort = false
                callbacks[1]()
            end
            return true, 0, "", 5, 6, "17", "test-epoch", "30000"
        end
        if function_name == "discard_acquired" or function_name == "release" then
            if function_name == "release" and fail_release_worker then
                return false, "injected worker dispatch failure"
            end
            if function_name == "release" and yield_release then
                yield_release = false
                coroutine.yield("release_worker_pending")
            end
            release_count = release_count + 1
            return true, 0, native_release_outcome, ""
        end
        if function_name == "close_provider" then return true, true, "closed" end
        error("unexpected worker function " .. function_name)
    end,
}

local native_bindings = require "dilock_etcd_ffi"
local fake_library = setmetatable({}, { __index = function(_, name)
    error("direct C ABI call outside worker thread: " .. name, 2)
end })
local B = {
    ffi = native_bindings.ffi,
    lib = fake_library,
    address_pair = native_bindings.address_pair,
    pointer_from = native_bindings.pointer_from,
    message = native_bindings.message,
    decimal_i64 = native_bindings.decimal_i64,
}
package.loaded["dilock_etcd_ffi"] = B
local pointer = ffi.cast("void *", 18364758544493064720ULL)
local high, low = B.address_pair(pointer)
assert(high == 4275878552 and low == 1985229328, "pointer halves retain exact bits")
assert(ffi.cast("uint64_t", B.pointer_from("void *", high, low)) ==
    ffi.cast("uint64_t", pointer), "pointer pair reconstructs exactly")

local dilock = require "dilock_etcd"
local bad, err = dilock.new({ epoch = "x\0y" })
assert(not bad and err == "invalid_epoch")
bad, err = dilock.new({ epoch = "test-epoch", rpc_timeout_ms = -1 })
assert(not bad and err == "invalid_rpc_timeout_ms")
local before_new = #calls
local provider = assert(dilock.new({ epoch = "test-epoch" }))
assert(#calls == before_new, "provider config construction performs no C ABI worker calls")
local lock = assert(provider:lock("resource"))
local none, outcome = lock:try_acquire()
assert(none == nil and outcome:find("unsupported_capability", 1, true))

local before = #calls
local result, scope_err, release_outcome = provider:with_lock("resource", function()
    error("injected scope failure")
end)
assert(result == nil and scope_err:find("injected scope failure", 1, true))
assert(release_outcome == "released")
local expected_scope_calls = {
    "abi_version", "create_provider", "create_lock", "create_cancellation",
    "acquire", "destroy_cancellation", "release",
}
assert(#calls == before + #expected_scope_calls,
    "lazy ABI validation and scope error dispatch exactly the expected worker operations")
for index, expected_name in ipairs(expected_scope_calls) do
    assert(calls[before + index].name == expected_name,
        "worker operation " .. index .. " must be " .. expected_name)
end

local value = assert(provider:with_lock("resource", function(lease)
    assert(lease.fence.create_revision == "17", "fence stays an exact decimal string")
    assert(lease.fence.coordination_epoch == "test-epoch")
    assert(lease.effective_ttl_ms == 30000)
    assert(lease:is_valid(), "lease validity check runs through the worker facade")
    return "completed"
end))
assert(value == "completed")
local arity_a, arity_b, arity_c, arity_d = provider:with_lock("resource", function()
    return nil, "detail", nil, "tail"
end)
assert(arity_a == nil and arity_b == "detail" and arity_c == nil and arity_d == "tail",
    "with_lock preserves callback result arity and nil holes")
assert(provider:with_lock("second-resource", function() return true end))

native_release_outcome = 1
local lost_value, lost_error, lost_outcome = provider:with_lock("lost-scope", function()
    return "must-not-be-reported-as-complete"
end)
assert(lost_value == nil and lost_error == "lease_lost" and lost_outcome == "already_not_owned",
    "with_lock must not return successful work when release confirms the lease is no longer owned")

local body_error = "application failure remains primary"
local failed_value, failed_error, failed_outcome = provider:with_lock("lost-body", function()
    error(body_error)
end)
assert(failed_value == nil and failed_error:find(body_error, 1, true) and failed_outcome == "already_not_owned",
    "lease-loss cleanup must not replace a callback failure")

native_release_outcome = 2
local unknown_value, unknown_error, unknown_release_err = provider:with_lock("resource", function()
    return "must-not-look-protected"
end)
assert(unknown_value == nil and unknown_error == "release_unknown" and unknown_release_err == "",
    "unknown etcd cleanup must not report successful scoped work")

local unknown_body_value, unknown_body_error, unknown_body_outcome = provider:with_lock("resource", function()
    error(body_error)
end)
assert(unknown_body_value == nil and unknown_body_error:find(body_error, 1, true)
    and unknown_body_outcome == "unknown",
    "unknown cleanup must not replace a callback failure and must remain observable")
native_release_outcome = 0

local create_provider_calls, create_lock_calls = 0, 0
for _, call in ipairs(calls) do
    if call.name == "create_provider" then create_provider_calls = create_provider_calls + 1 end
    if call.name == "create_lock" then create_lock_calls = create_lock_calls + 1 end
end
assert(create_provider_calls == 1 and create_lock_calls == 4,
    "different lock handles share one native provider and each resource gets one native handle")
local created_resources = {}
for _, call in ipairs(calls) do
    if call.name == "create_lock" then
        local resource = call.args[3]
        assert(not created_resources[resource], "resource handle must be reused: " .. resource)
        created_resources[resource] = true
    end
end
assert(created_resources.resource and created_resources["second-resource"]
    and created_resources["lost-scope"] and created_resources["lost-body"],
    "each distinct scoped resource gets a native handle")

local old_release_count = release_count
local saw_abort = false
local scoped = provider:with_lock("resource", function(lease)
    callbacks[1]()
    saw_abort = lease:abort_requested()
    assert(release_count == old_release_count, "disconnect never releases while protected callback is still running")
    return "finished-after-cooperative-abort"
end)
assert(saw_abort and scoped == "finished-after-cooperative-abort")
assert(release_count == old_release_count + 1, "scope releases exactly once after callback returns")

ngx.ctx = {}
state.ctx = ngx.ctx
callbacks = {}
trigger_abort = true
local aborted, abort_err = lock:acquire({ wait_ms = 10, lease_ms = 1000 })
assert(aborted == nil and abort_err:find("cancelled", 1, true))
assert(calls[#calls].name == "discard_acquired", "acquire/abort race disposes the returned lease")
assert(#callbacks == 1, "one abort callback is registered per request")
assert(cancellation_requests == 1, "abort signals cancellation through the configured worker pool")
assert(cancellation_destroys >= 1, "cancellation handles are destroyed through the worker pool")

-- ngx.ctx is request-local. The preceding simulated disconnect must not leak
-- its aborted state into the independent overlapping-release scenario.
ngx.ctx = {}
state.ctx = ngx.ctx
callbacks = {}
local concurrent_lease = assert(assert(provider:lock("concurrent-release")):acquire())
local release_calls_before = #calls
local releases_before = release_count
yield_release = true
local release_coroutine = coroutine.create(function() return concurrent_lease:release() end)
local resumed, yielded = coroutine.resume(release_coroutine)
assert(resumed and yielded == "release_worker_pending", "first release yields with its native call in flight")
local overlapping_release, overlapping_error = concurrent_lease:release()
assert(overlapping_release == "unknown" and overlapping_error == "lease_operation_in_progress",
    "overlapping release must not dispatch a second C ABI call against the same lease pointer")
local overlapping_validity, validity_error = concurrent_lease:is_valid()
assert(overlapping_validity == nil and validity_error == "lease_operation_in_progress",
    "validity checks must not race a release that can destroy the native handle")
assert(#calls == release_calls_before + 1 and calls[#calls].name == "release",
    "overlapping lease operations do not dispatch another worker call")
local yielded_again
resumed, yielded_again = coroutine.resume(release_coroutine)
assert(resumed and yielded_again == "released", "the original release completes with its acknowledgement")
assert(release_count == releases_before + 1, "the native lease is released and destroyed exactly once")
assert(concurrent_lease:release() == "released", "later release observes the cached terminal outcome")

assert(provider:close(), "explicit close destroys native lock handles before shared provider")
local close_count = 0
for _, call in ipairs(calls) do if call.name == "close_provider" then close_count = close_count + 1 end end
assert(close_count == 1, "shared provider closes once")

ngx.ctx = {}
state.ctx = ngx.ctx
callbacks = {}
local retry_provider = assert(dilock.new({ epoch = "test-epoch" }))
local retry_lock = assert(retry_provider:lock("retry-release"))
local retry_lease = assert(retry_lock:acquire())
fail_release_worker = true
local unknown, dispatch_err = retry_lease:release()
assert(unknown == "unknown" and dispatch_err:find("injected worker dispatch failure", 1, true))
local closed, close_err = retry_provider:close()
assert(not closed and close_err:find("provider_busy", 1, true),
    "close cannot destroy native handles after uncertain worker dispatch")
fail_release_worker = false

local invalid_outcome_provider = assert(dilock.new({ epoch = "test-epoch" }))
local invalid_outcome_lease = assert(assert(invalid_outcome_provider:lock("bad-release-outcome")):acquire())
native_release_outcome = 99
local invalid_outcome, invalid_outcome_err = invalid_outcome_lease:release()
assert(invalid_outcome == "unknown" and invalid_outcome_err:find("protocol_mismatch", 1, true),
    "an unknown C ABI release enum must not be reported as AlreadyNotOwned")
native_release_outcome = 0
assert(invalid_outcome_provider:close(), "provider closes after invalid result was classified as unknown")
fail_release_worker = false
assert(retry_lease:release() == "released", "explicit retry succeeds after worker scheduling recovers")
assert(retry_provider:close())

ngx.ctx = {}
state.ctx = ngx.ctx
ngx.on_abort = nil
local unmonitored, monitor_err = lock:acquire()
assert(not unmonitored and monitor_err:find("openresty_request_context_required", 1, true),
    "acquisition fails closed without request abort support")

print("PASS: Lua etcd facade regression tests")
