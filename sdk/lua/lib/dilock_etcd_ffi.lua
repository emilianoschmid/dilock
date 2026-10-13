-- Native C ABI bindings for the experimental OpenResty etcd adapter.
local ffi = require "ffi"

ffi.cdef[[
typedef int32_t dilock_status;
typedef int32_t dilock_release_outcome;
typedef struct dilock_error { int32_t code; char message[384]; } dilock_error;
typedef struct dilock_cancellation dilock_cancellation;
typedef struct dilock_etcd_provider dilock_etcd_provider;
typedef struct dilock_etcd_lock dilock_etcd_lock;
typedef struct dilock_etcd_lease dilock_etcd_lease;
typedef struct dilock_etcd_fence { int64_t create_revision; size_t coordination_epoch_bytes; } dilock_etcd_fence;
typedef struct dilock_etcd_options {
  const char *endpoint; const char *name_space; const char *coordination_epoch;
  const char *tls_root_certificates; const char *tls_client_certificate;
  const char *tls_client_private_key; void *cleanup_callback; void *cleanup_user_data;
  int64_t rpc_timeout_ms; int64_t cleanup_timeout_ms; int64_t safety_margin_ms;
  int64_t default_wait_ms; int64_t default_lease_ms; int32_t auto_renew;
} dilock_etcd_options;
uint32_t dilock_c_abi_version(void);
void dilock_etcd_options_init(dilock_etcd_options *options);
dilock_status dilock_etcd_provider_create(const dilock_etcd_options *, dilock_etcd_provider **, dilock_error *);
void dilock_etcd_provider_destroy(dilock_etcd_provider *);
dilock_status dilock_etcd_lock_create(dilock_etcd_provider *, const char *, dilock_etcd_lock **, dilock_error *);
void dilock_etcd_lock_destroy(dilock_etcd_lock *);
dilock_cancellation *dilock_cancellation_create(dilock_error *);
void dilock_cancellation_request(dilock_cancellation *);
void dilock_cancellation_destroy(dilock_cancellation *);
dilock_status dilock_etcd_acquire_with_cancellation(dilock_etcd_lock *, int64_t, int64_t, dilock_cancellation *, dilock_etcd_lease **, dilock_error *);
int32_t dilock_etcd_lease_is_valid(const dilock_etcd_lease *);
dilock_status dilock_etcd_lease_effective_ttl_ms(const dilock_etcd_lease *, int64_t *, dilock_error *);
dilock_status dilock_etcd_lease_fence(const dilock_etcd_lease *, dilock_etcd_fence *, char *, size_t, dilock_error *);
dilock_status dilock_etcd_lease_release(dilock_etcd_lease *, dilock_release_outcome *, dilock_error *);
void dilock_etcd_lease_destroy(dilock_etcd_lease *);
]]

local M = { ffi = ffi }
M.lib = ffi.load(os.getenv("DILOCK_C_LIBRARY") or "dilock_c")
local TWO32 = 4294967296ULL
function M.address_pair(pointer)
    local raw = ffi.cast("uint64_t", pointer)
    return tonumber(raw / TWO32), tonumber(raw % TWO32)
end
function M.pointer_from(ctype, high, low)
    assert(type(high) == "number" and high >= 0 and high < 4294967296 and high % 1 == 0)
    assert(type(low) == "number" and low >= 0 and low < 4294967296 and low % 1 == 0)
    local raw = ffi.cast("uint64_t", high) * TWO32 + ffi.cast("uint64_t", low)
    return ffi.cast(ctype, raw)
end
function M.message(error) return ffi.string(error.message) end
function M.decimal_i64(value) return tostring(value):gsub("LL$", ""):gsub("ULL$", "") end
return M
