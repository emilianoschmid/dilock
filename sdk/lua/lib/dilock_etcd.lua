-- Experimental OpenResty facade for Dilock's native etcd C ABI.
local B = require "dilock_etcd_ffi"
local semaphore = require "ngx.semaphore"
local lib = B.lib
local M, Provider, Lock, Lease = {}, {}, {}, {}
Provider.__index, Lock.__index, Lease.__index = Provider, Lock, Lease
local provider_registry = {}
local worker

local function integer(value, name, min, max)
    if type(value) ~= "number" or value % 1 ~= 0 or value < min or value > max then
        return nil, "invalid_" .. name
    end
    return value
end
local function status_error(status, message)
    local category = ({ [2] = "invalid_argument", [3] = "cancelled", [4] = "wait_timeout",
        [5] = "backend_unavailable", [6] = "acquisition_unknown", [7] = "protocol_mismatch",
        [8] = "lease_lost", [9] = "unsupported_capability", [10] = "internal" })[status] or "backend_error"
    return category .. (message and message ~= "" and (": " .. message) or "")
end
local function pack(...)
    return { n = select("#", ...), ... }
end

local function request_abort_state()
    if not ngx or not ngx.ctx or not ngx.on_abort or not ngx.run_worker_thread then
        return nil, "openresty_request_context_required"
    end
    local ctx = ngx.ctx
    if ctx.dilock_etcd_abort_state then return ctx.dilock_etcd_abort_state end
    local state = { aborted = false, pending = {}, active_leases = {} }
    local ok, err = ngx.on_abort(function()
        state.aborted = true
        -- Keep every C ABI call in the configured worker pool. The worker
        -- request is awaited before acquire can destroy its cancellation token.
        for _, pending in pairs(state.pending) do
            local requested, request_err = worker(
                pending.thread_pool, "request_cancellation", pending.high, pending.low)
            if requested ~= true then
                state.cancellation_errors = state.cancellation_errors or {}
                state.cancellation_errors[#state.cancellation_errors + 1] =
                    tostring(request_err or "cancellation worker failed")
            end
        end
        -- A downstream disconnect does not prove that application work stopped.
        -- Mark active scopes for cooperative cancellation, but never release
        -- their leases concurrently with a still-running callback.
        for lease in pairs(state.active_leases) do lease._abort_requested = true end
    end)
    if not ok then return nil, "client_abort_monitor_unavailable: " .. tostring(err) end
    ctx.dilock_etcd_abort_state = state
    return state
end

worker = function(pool, name, ...)
    local ok, a, b, c, d, e, f, g, h, i, j, k = ngx.run_worker_thread(
        pool, "dilock_etcd_worker", name, ...)
    if not ok then return nil, "worker_error: " .. tostring(a) end
    return a, b, c, d, e, f, g, h, i, j, k
end

function M.new(config)
    config = config or {}
    local namespace = config.namespace or "default"
    local endpoint = config.endpoint or "127.0.0.1:2379"
    if type(namespace) ~= "string" or not namespace:match("^[a-z0-9][a-z0-9_.%-]*$") or #namespace > 64 then
        return nil, "invalid_namespace"
    end
    if type(config.epoch) ~= "string" or #config.epoch == 0 or #config.epoch > 1024
        or config.epoch:find("%z") then return nil, "invalid_epoch" end
    if type(endpoint) ~= "string" or #endpoint == 0 or endpoint:find("%z") then return nil, "invalid_endpoint" end
    local rpc, err = integer(config.rpc_timeout_ms or 2000, "rpc_timeout_ms", 1, 120000)
    if not rpc then return nil, err end
    local cleanup; cleanup, err = integer(config.cleanup_timeout_ms or 2000, "cleanup_timeout_ms", 1, 120000)
    if not cleanup then return nil, err end
    local margin; margin, err = integer(config.safety_margin_ms or 100, "safety_margin_ms", 0, 60000)
    if not margin then return nil, err end
    local wait; wait, err = integer(config.default_wait_ms or 5000, "default_wait_ms", 0, 86400000)
    if not wait then return nil, err end
    local lease; lease, err = integer(config.default_lease_ms or 30000, "default_lease_ms", 1000, 86400000)
    if not lease then return nil, err end
    local thread_pool = config.thread_pool or "dilock"
    local provider = setmetatable({
        thread_pool = thread_pool,
        config = { endpoint = endpoint, namespace = namespace, epoch = config.epoch,
            tls_root_certificates = config.tls_root_certificates,
            tls_client_certificate = config.tls_client_certificate,
            tls_client_private_key = config.tls_client_private_key,
            rpc_timeout_ms = rpc, cleanup_timeout_ms = cleanup, safety_margin_ms = margin,
            default_wait_ms = wait, default_lease_ms = lease, auto_renew = config.auto_renew ~= false },
        _native_locks = {}, _inflight = 0, _active_leases = 0,
        _closed = false, _abi_checked = false,
    }, Provider)
    provider_registry[#provider_registry + 1] = provider
    return provider
end

local function gate(provider)
    if not provider._gate then
        local sem, err = semaphore.new(1)
        if not sem then return nil, "ngx_semaphore_unavailable: " .. tostring(err) end
        provider._gate = sem
    end
    return provider._gate
end

function Provider:_ensure_lock(resource)
    local sem, err = gate(self)
    if not sem then return nil, err end
    local ok; ok, err = sem:wait(30)
    if not ok then return nil, "provider_gate_timeout: " .. tostring(err) end
    if self._closed then sem:post(); return nil, "provider_closed" end
    if not self._abi_checked then
        local abi, abi_err = worker(self.thread_pool, "abi_version")
        if abi == nil then sem:post(); return nil, abi_err end
        if tonumber(abi) ~= 1 then
            sem:post()
            return nil, "unsupported_c_abi_version: " .. tostring(abi)
        end
        self._abi_checked = true
    end
    if not self._native_hi then
        local status, message, hi, lo = worker(self.thread_pool, "create_provider", self.config)
        if status == nil or status ~= 0 then sem:post(); return nil, status == nil and message or status_error(status, message) end
        self._native_hi, self._native_lo = hi, lo
    end
    local pair = self._native_locks[resource]
    if not pair then
        local status, message, hi, lo = worker(self.thread_pool, "create_lock",
            self._native_hi, self._native_lo, resource)
        if status == nil or status ~= 0 then sem:post(); return nil, status == nil and message or status_error(status, message) end
        pair = { hi, lo }
        self._native_locks[resource] = pair
    end
    self._inflight = self._inflight + 1
    sem:post()
    return { provider_hi = self._native_hi, provider_lo = self._native_lo,
        lock_hi = pair[1], lock_lo = pair[2] }
end

function Provider:_finish_acquire(acquired)
    local sem = assert(gate(self))
    assert(sem:wait(30))
    self._inflight = self._inflight - 1
    if acquired then self._active_leases = self._active_leases + 1 end
    sem:post()
end

function Provider:_finish_lease()
    local sem = assert(gate(self))
    assert(sem:wait(30))
    self._active_leases = math.max(0, self._active_leases - 1)
    sem:post()
end

function Provider:close()
    if self._closed then return true end
    local exiting = ngx and ngx.get_phase and ngx.get_phase() == "exit_worker"
    -- exit_worker runs after nginx has drained this worker's requests. A
    -- request semaphore cannot be waited there, so its counters are stable.
    if exiting then
        if self._inflight > 0 or self._active_leases > 0 then
            return nil, "provider_busy_at_worker_exit"
        end
        if self._native_hi then
            -- ngx.run_worker_thread is explicitly disabled by ngx_lua in this
            -- phase. Requests have drained and counters are zero, so invoke
            -- only local C++ destructors here; no backend RPC is performed.
            for _, pair in pairs(self._native_locks) do
                lib.dilock_etcd_lock_destroy(B.pointer_from("dilock_etcd_lock *", pair[1], pair[2]))
            end
            lib.dilock_etcd_provider_destroy(
                B.pointer_from("dilock_etcd_provider *", self._native_hi, self._native_lo))
        end
        self._native_hi, self._native_lo, self._native_locks = nil, nil, {}
        self._closed = true
        return true
    end
    local sem, err = gate(self)
    if not sem then return nil, err end
    local ok; ok, err = sem:wait(30)
    if not ok then return nil, "provider_gate_timeout: " .. tostring(err) end
    if self._inflight > 0 or self._active_leases > 0 then
        sem:post()
        return nil, "provider_busy: release active leases and finish acquisitions before close"
    end
    if self._native_hi then
        local lock_pairs = {}
        for _, pair in pairs(self._native_locks) do lock_pairs[#lock_pairs + 1] = pair end
        local closed, message = worker(self.thread_pool, "close_provider",
            self._native_hi, self._native_lo, lock_pairs)
        if closed ~= true then sem:post(); return nil, message or "provider_close_unknown" end
    end
    self._native_hi, self._native_lo, self._native_locks = nil, nil, {}
    self._closed = true
    sem:post()
    return true
end

function M.shutdown_all()
    local errors = {}
    for i = #provider_registry, 1, -1 do
        local ok, err = provider_registry[i]:close()
        if not ok then errors[#errors + 1] = tostring(err) end
    end
    if #errors > 0 then return nil, table.concat(errors, "; ") end
    return true
end

function Provider:lock(resource)
    if type(resource) ~= "string" or #resource == 0 or #resource > 1024 or resource:find("%z") then
        return nil, "invalid_resource"
    end
    return setmetatable({ provider = self, resource = resource }, Lock)
end

function Lock:try_acquire()
    return nil, "unsupported_capability: native etcd Lock waits in queue; use acquire"
end

function Lock:acquire(options)
    options = options or {}
    local state, err = request_abort_state()
    if not state then return nil, err end
    if state.aborted then return nil, "cancelled" end
    local wait_ms; wait_ms, err = integer(options.wait_ms or self.provider.config.default_wait_ms, "wait_ms", 0, 86400000)
    if not wait_ms then return nil, err end
    local lease_ms; lease_ms, err = integer(options.lease_ms or self.provider.config.default_lease_ms, "lease_ms", 1000, 86400000)
    if not lease_ms then return nil, err end
    local handles; handles, err = self.provider:_ensure_lock(self.resource)
    if not handles then return nil, err end
    local cancel_status, cancel_message, cancel_hi, cancel_lo =
        worker(self.provider.thread_pool, "create_cancellation")
    if cancel_status == nil or cancel_status ~= 0 then
        self.provider:_finish_acquire(false)
        return nil, cancel_message or status_error(cancel_status, "could not create cancellation token")
    end
    local pending_key = tostring(cancel_hi) .. ":" .. tostring(cancel_lo)
    state.pending[pending_key] = {
        thread_pool = self.provider.thread_pool,
        high = cancel_hi,
        low = cancel_lo,
    }
    local status, message, vh, vl, revision, epoch, ttl =
        worker(self.provider.thread_pool, "acquire", handles.provider_hi, handles.provider_lo,
            handles.lock_hi, handles.lock_lo, cancel_hi, cancel_lo, wait_ms, lease_ms)
    state.pending[pending_key] = nil
    local destroyed, destroy_err = worker(
        self.provider.thread_pool, "destroy_cancellation", cancel_hi, cancel_lo)
    if destroyed ~= true then
        if status == 0 then
            self.provider:_finish_acquire(true)
            local cleanup_status, cleanup_outcome, cleanup_message =
                worker(self.provider.thread_pool, "discard_acquired", vh, vl)
            self.provider:_finish_lease()
            if cleanup_status == nil or cleanup_status ~= 0 or cleanup_outcome == 2 then
                return nil, "acquisition_unknown: cancellation handle cleanup failed and lease discard is unknown: " ..
                    tostring(cleanup_message or cleanup_outcome or destroy_err)
            end
            return nil, "internal: cancellation token destroy failed after acknowledged lease cleanup: " ..
                tostring(destroy_err)
        end
        self.provider:_finish_acquire(false)
        if status == nil then
            return nil, "acquisition_unknown: worker acquire result and cancellation token cleanup are unknown: " ..
                tostring(message or destroy_err)
        end
        return nil, status_error(status, message) ..
            "; cancellation token destroy failed: " .. tostring(destroy_err)
    end
    if status == nil then self.provider:_finish_acquire(false); return nil, message end
    if status ~= 0 then
        self.provider:_finish_acquire(false)
        return nil, status_error(status, message)
    end
    self.provider:_finish_acquire(true)
    if state.aborted then
        local cleanup_status, cleanup_outcome, cleanup_message =
            worker(self.provider.thread_pool, "discard_acquired", vh, vl)
        self.provider:_finish_lease()
        if cleanup_status == nil or cleanup_status ~= 0 or cleanup_outcome == 2 then
            return nil, "acquisition_unknown: aborted request; lease cleanup outcome unknown: " ..
                tostring(cleanup_message or cleanup_outcome)
        end
        return nil, "cancelled: request aborted after acquisition; lease cleanup acknowledged"
    end
    local lease = setmetatable({ provider = self.provider, thread_pool = self.provider.thread_pool,
        provider_hi = handles.provider_hi, provider_lo = handles.provider_lo,
        lock_hi = handles.lock_hi, lock_lo = handles.lock_lo,
        lease_hi = vh, lease_lo = vl, fence = { create_revision = revision, coordination_epoch = epoch },
        effective_ttl_ms = tonumber(ttl), release_outcome = nil }, Lease)
    state.active_leases[lease] = true
    lease._request_state = state
    return lease
end

function Lease:is_valid()
    if self.released then return false end
    if self._native_operation then return nil, "lease_operation_in_progress" end
    self._native_operation = "validity check"
    local dispatched, valid, err = pcall(worker, self.thread_pool, "lease_valid",
        self.lease_hi, self.lease_lo)
    self._native_operation = nil
    if not dispatched then return nil, "worker_error: " .. tostring(valid) end
    if valid == nil then return nil, err end
    return valid == true
end

function Lease:abort_requested()
    return self._abort_requested == true
end

function Lease:release()
    if self.release_outcome then return self.release_outcome, self.release_error end
    if self._native_operation then return "unknown", "lease_operation_in_progress" end
    self._native_operation = "release"
    local dispatched, status, outcome, message = pcall(worker, self.thread_pool, "release",
        self.lease_hi, self.lease_lo)
    self._native_operation = nil
    if not dispatched then
        self.release_error = "worker_error: " .. tostring(status)
        return "unknown", self.release_error
    end
    if status == nil then
        -- The worker may not have started. Preserve every native handle and
        -- permit retry; close stays fail-closed while this lease is active.
        self.release_error = outcome
        return "unknown", self.release_error
    elseif status ~= 0 or outcome == 2 then
        self.release_outcome, self.release_error = "unknown", message
    elseif outcome == 0 then
        self.release_outcome, self.release_error = "released", nil
    elseif outcome == 1 then
        self.release_outcome, self.release_error = "already_not_owned", nil
    else
        self.release_outcome = "unknown"
        self.release_error = "protocol_mismatch: C ABI returned an invalid release outcome"
    end
    self.released = true
    if self._request_state then self._request_state.active_leases[self] = nil end
    self.provider:_finish_lease()
    return self.release_outcome, self.release_error
end

function Provider:with_lock(resource, options, fn)
    if type(options) == "function" and fn == nil then fn, options = options, {} end
    if type(fn) ~= "function" then return nil, "callback_required" end
    local lock, err = self:lock(resource)
    if not lock then return nil, err end
    local lease; lease, err = lock:acquire(options)
    if not lease then return nil, err end
    local values = pack(xpcall(function() return fn(lease) end, debug.traceback))
    local outcome, release_err = lease:release()
    if not values[1] then return nil, values[2], outcome, release_err, lease end
    if outcome == "unknown" then return nil, "release_unknown", release_err, lease end
    if outcome ~= "released" then return nil, "lease_lost", outcome, release_err, lease end
    for i = 1, values.n - 1 do values[i] = values[i + 1] end
    values.n = values.n - 1
    return unpack(values, 1, values.n)
end

return M
