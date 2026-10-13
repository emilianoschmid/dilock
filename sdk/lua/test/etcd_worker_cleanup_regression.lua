-- Exercise failure after native etcd acquisition with a deterministic C ABI double.
local ffi = require "ffi"
package.path = "/repo/sdk/lua/lib/?.lua;" .. package.path
ffi.cdef[[
typedef int32_t dilock_status;
typedef int32_t dilock_release_outcome;
typedef struct dilock_error { int32_t code; char message[384]; } dilock_error;
typedef struct dilock_etcd_provider dilock_etcd_provider;
typedef struct dilock_etcd_lock dilock_etcd_lock;
typedef struct dilock_cancellation dilock_cancellation;
typedef struct dilock_etcd_lease dilock_etcd_lease;
typedef struct dilock_etcd_fence { int64_t create_revision; size_t coordination_epoch_bytes; } dilock_etcd_fence;
]]

local state = { accessor = "fence", release_status = 5, release_outcome = 2,
    release_message = "injected cleanup acknowledgement loss", destroyed = 0 }
local function write_error(error, code, message)
    error[0].code = code
    ffi.copy(error[0].message, message, #message)
end
local lease_pointer = ffi.cast("dilock_etcd_lease *", 0x12345000)
local fake_library = {
    dilock_etcd_acquire_with_cancellation = function(_, _, _, _, out)
        out[0] = lease_pointer
        return 0
    end,
    dilock_etcd_lease_fence = function(_, _, _, _, error)
        if state.accessor == "fence" then
            write_error(error, 7, "injected fence accessor failure")
            return 7
        end
        return 0
    end,
    dilock_etcd_lease_effective_ttl_ms = function(_, _, error)
        write_error(error, 8, "injected TTL accessor failure")
        return 8
    end,
    dilock_etcd_lease_release = function(_, outcome, error)
        outcome[0] = state.release_outcome
        if state.release_status ~= 0 then write_error(error, state.release_status, state.release_message) end
        return state.release_status
    end,
    dilock_etcd_lease_destroy = function(pointer)
        assert(pointer == lease_pointer)
        state.destroyed = state.destroyed + 1
    end,
}
local function address_pair(pointer)
    local raw = ffi.cast("uint64_t", pointer)
    return tonumber(raw / 4294967296ULL), tonumber(raw % 4294967296ULL)
end
local function pointer_from(ctype, high, low)
    local raw = ffi.cast("uint64_t", high) * 4294967296ULL + ffi.cast("uint64_t", low)
    return ffi.cast(ctype, raw)
end
package.loaded["dilock_etcd_ffi"] = {
    ffi = ffi, lib = fake_library, address_pair = address_pair,
    pointer_from = pointer_from,
    message = function(error) return ffi.string(error.message) end,
    decimal_i64 = function(value) return tostring(value) end,
}

local worker = require "dilock_etcd_worker"
local status, message = worker.acquire(1, 2, 3, 4, 5, 6, 100, 1000)
assert(status == 6, "unacknowledged cleanup after acquisition must become acquisition_unknown")
assert(message:find("injected fence accessor failure", 1, true), "original accessor failure must be retained")
assert(message:find("injected cleanup acknowledgement loss", 1, true), "cleanup failure must be visible")
assert(state.destroyed == 1, "native lease handle must be destroyed once")

state.release_status, state.release_outcome, state.release_message = 0, 2, ""
state.destroyed = 0
status, message = worker.acquire(1, 2, 3, 4, 5, 6, 100, 1000)
assert(status == 6 and message:find("outcome 2", 1, true),
    "a successful C ABI call with Unknown cleanup outcome must remain acquisition_unknown")
assert(state.destroyed == 1, "unknown release outcome still destroys the native handle once")

state.accessor = "ttl"
state.release_status, state.release_outcome, state.release_message = 0, 0, ""
state.destroyed = 0
status, message = worker.acquire(1, 2, 3, 4, 5, 6, 100, 1000)
assert(status == 8, "acknowledged cleanup must preserve the TTL accessor status")
assert(message == "injected TTL accessor failure", "cleanup must not overwrite the original accessor error")
assert(state.destroyed == 1, "acknowledged cleanup still destroys the native lease once")

print("PASS: Lua etcd worker post-acquisition cleanup regression")
