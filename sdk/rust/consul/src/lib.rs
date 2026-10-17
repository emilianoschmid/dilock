//! Rust ownership facade for the optional Dilock Consul C ABI profile.
//!
//! The native `dilock_c` library must be built with `DILOCK_BUILD_CONSUL=ON`
//! and `DILOCK_BUILD_C_API=ON`. Calls are synchronous and bounded by the
//! supplied operation budgets. Async applications should run blocking calls
//! on their runtime's blocking pool. Handles are intentionally neither
//! `Send` nor `Sync`; do not move them across threads.

use std::{
    ffi::{CString, c_char, c_int, c_void},
    marker::PhantomData,
    ptr::{self, NonNull},
    rc::Rc,
    time::Duration,
};

const ABI_VERSION: u32 = 1;
const STATUS_BUSY: i32 = 1;
const STATUS_INVALID_ARGUMENT: i32 = 2;
const ERROR_MESSAGE_CAPACITY: usize = 384;

#[repr(C)]
struct CError {
    code: i32,
    message: [c_char; ERROR_MESSAGE_CAPACITY],
}

#[repr(C)]
struct CConsulOptions {
    endpoint: *const c_char,
    name_space: *const c_char,
    coordination_epoch: *const c_char,
    acl_token: *const c_char,
    cleanup_callback: Option<unsafe extern "C" fn(i32, i32, *const c_char, *mut c_void)>,
    cleanup_user_data: *mut c_void,
    rpc_timeout_ms: i64,
    cleanup_timeout_ms: i64,
    poll_interval_ms: i64,
    default_wait_ms: i64,
    default_lease_ms: i64,
}

#[repr(C)]
struct CConsulFence {
    lock_index: u64,
    coordination_epoch_bytes: usize,
}

#[repr(C)]
struct CProvider {
    _opaque: [u8; 0],
}
#[repr(C)]
struct CLock {
    _opaque: [u8; 0],
}
#[repr(C)]
struct CLease {
    _opaque: [u8; 0],
}
#[repr(C)]
struct CCancellation {
    _opaque: [u8; 0],
}

#[link(name = "dilock_c")]
unsafe extern "C" {
    fn dilock_c_abi_version() -> u32;
    fn dilock_consul_options_init(options: *mut CConsulOptions);
    fn dilock_cancellation_create(error: *mut CError) -> *mut CCancellation;
    fn dilock_cancellation_request(cancellation: *mut CCancellation);
    fn dilock_cancellation_destroy(cancellation: *mut CCancellation);
    fn dilock_consul_provider_create(
        options: *const CConsulOptions,
        out: *mut *mut CProvider,
        error: *mut CError,
    ) -> c_int;
    fn dilock_consul_provider_destroy(provider: *mut CProvider);
    fn dilock_consul_lock_create(
        provider: *mut CProvider,
        resource: *const c_char,
        out: *mut *mut CLock,
        error: *mut CError,
    ) -> c_int;
    fn dilock_consul_lock_destroy(lock: *mut CLock);
    fn dilock_consul_acquire(
        lock: *mut CLock,
        wait_ms: i64,
        lease_ms: i64,
        out: *mut *mut CLease,
        error: *mut CError,
    ) -> c_int;
    fn dilock_consul_acquire_default(
        lock: *mut CLock,
        out: *mut *mut CLease,
        error: *mut CError,
    ) -> c_int;
    fn dilock_consul_acquire_with_cancellation(
        lock: *mut CLock,
        wait_ms: i64,
        lease_ms: i64,
        cancellation: *mut CCancellation,
        out: *mut *mut CLease,
        error: *mut CError,
    ) -> c_int;
    fn dilock_consul_acquire_default_with_cancellation(
        lock: *mut CLock,
        cancellation: *mut CCancellation,
        out: *mut *mut CLease,
        error: *mut CError,
    ) -> c_int;
    fn dilock_consul_try_acquire(
        lock: *mut CLock,
        lease_ms: i64,
        out: *mut *mut CLease,
        error: *mut CError,
    ) -> c_int;
    fn dilock_consul_lease_is_valid(lease: *const CLease) -> i32;
    fn dilock_consul_lease_effective_ttl_ms(
        lease: *const CLease,
        ttl_ms: *mut i64,
        error: *mut CError,
    ) -> c_int;
    fn dilock_consul_lease_fence(
        lease: *const CLease,
        fence: *mut CConsulFence,
        epoch: *mut c_char,
        epoch_capacity: usize,
        error: *mut CError,
    ) -> c_int;
    fn dilock_consul_lease_renew(
        lease: *mut CLease,
        renewed: *mut i32,
        error: *mut CError,
    ) -> c_int;
    fn dilock_consul_lease_release(
        lease: *mut CLease,
        outcome: *mut i32,
        error: *mut CError,
    ) -> c_int;
    fn dilock_consul_lease_destroy(lease: *mut CLease);
    fn dilock_consul_provision_namespace(options: *const CConsulOptions, error: *mut CError)
        -> c_int;
}

/// A native C ABI failure. The message is copied before returning to Rust.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    AbiMismatch,
    InvalidArgument(String),
    Busy,
    Cancelled,
    WaitTimeout,
    BackendUnavailable(String),
    AcquisitionUnknown,
    ProtocolMismatch(String),
    LeaseLost,
    UnsupportedCapability,
    Internal(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AbiMismatch => f.write_str("Dilock C ABI version mismatch"),
            Self::InvalidArgument(message) => write!(f, "invalid argument: {message}"),
            Self::Busy => f.write_str("resource is currently held"),
            Self::Cancelled => f.write_str("operation was cancelled"),
            Self::WaitTimeout => f.write_str("acquisition wait timed out"),
            Self::BackendUnavailable(message) => write!(f, "backend unavailable: {message}"),
            Self::AcquisitionUnknown => f.write_str("acquisition outcome is unknown"),
            Self::ProtocolMismatch(message) => write!(f, "Consul metadata mismatch: {message}"),
            Self::LeaseLost => f.write_str("lease is no longer locally valid"),
            Self::UnsupportedCapability => f.write_str("requested capability is unsupported"),
            Self::Internal(message) => write!(f, "Dilock internal error: {message}"),
        }
    }
}

impl std::error::Error for Error {}

/// Endpoint, metadata identity and timing values for the Consul profile.
#[derive(Clone)]
pub struct ConsulOptions {
    pub endpoint: String,
    pub namespace: String,
    pub coordination_epoch: String,
    pub acl_token: String,
    pub rpc_timeout: Duration,
    pub cleanup_timeout: Duration,
    pub poll_interval: Duration,
    pub default_wait: Duration,
    pub default_lease: Duration,
}

impl std::fmt::Debug for ConsulOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsulOptions")
            .field("endpoint", &self.endpoint)
            .field("namespace", &self.namespace)
            .field("coordination_epoch", &self.coordination_epoch)
            .field("acl_token", &"[REDACTED]")
            .field("rpc_timeout", &self.rpc_timeout)
            .field("cleanup_timeout", &self.cleanup_timeout)
            .field("poll_interval", &self.poll_interval)
            .field("default_wait", &self.default_wait)
            .field("default_lease", &self.default_lease)
            .finish()
    }
}

impl Default for ConsulOptions {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:8500".into(),
            namespace: "default".into(),
            coordination_epoch: String::new(),
            acl_token: String::new(),
            rpc_timeout: Duration::from_secs(2),
            cleanup_timeout: Duration::from_secs(2),
            poll_interval: Duration::from_millis(50),
            default_wait: Duration::from_secs(5),
            default_lease: Duration::from_secs(30),
        }
    }
}

/// Fence returned after Consul confirms this session owns the key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsulFence {
    pub coordination_epoch: String,
    pub lock_index: u64,
}

/// Release acknowledgement returned by the native C ABI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseOutcome {
    Released,
    AlreadyNotOwned,
    Unknown,
}

/// Provisions namespace metadata explicitly; acquisition never repairs it.
pub fn provision_namespace(options: &ConsulOptions) -> Result<(), Error> {
    check_abi()?;
    let native = native_options(options)?;
    let mut error = empty_error();
    let status = unsafe { dilock_consul_provision_namespace(&native.value, &mut error) };
    status_result(status, &error)
}

/// Reusable selection of one Consul endpoint and metadata epoch.
pub struct ConsulProvider {
    raw: NonNull<CProvider>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl ConsulProvider {
    pub fn new(options: &ConsulOptions) -> Result<Self, Error> {
        check_abi()?;
        let native = native_options(options)?;
        let mut error = empty_error();
        let mut raw = ptr::null_mut();
        let status = unsafe { dilock_consul_provider_create(&native.value, &mut raw, &mut error) };
        status_result(status, &error)?;
        Ok(Self {
            raw: NonNull::new(raw).ok_or_else(|| Error::Internal("provider handle was null".into()))?,
            _not_send_sync: PhantomData,
        })
    }

    pub fn lock(&self, resource: &str) -> Result<ConsulHandle, Error> {
        let resource = c_string(resource)?;
        let mut error = empty_error();
        let mut raw = ptr::null_mut();
        let status = unsafe {
            dilock_consul_lock_create(self.raw.as_ptr(), resource.as_ptr(), &mut raw, &mut error)
        };
        status_result(status, &error)?;
        Ok(ConsulHandle {
            raw: NonNull::new(raw).ok_or_else(|| Error::Internal("lock handle was null".into()))?,
            _not_send_sync: PhantomData,
        })
    }
}

impl Drop for ConsulProvider {
    fn drop(&mut self) {
        unsafe { dilock_consul_provider_destroy(self.raw.as_ptr()) }
    }
}

/// Reusable named resource handle.
pub struct ConsulHandle {
    raw: NonNull<CLock>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl ConsulHandle {
    pub fn acquire_default(&self) -> Result<ConsulLease, Error> {
        self.acquire_with(None, None, None)
    }

    pub fn acquire(&self, wait: Duration, lease: Duration) -> Result<ConsulLease, Error> {
        self.acquire_with(Some(wait), Some(lease), None)
    }

    pub fn acquire_default_cancellable(
        &self,
        cancellation: &Cancellation,
    ) -> Result<ConsulLease, Error> {
        self.acquire_with(None, None, Some(cancellation))
    }

    pub fn acquire_cancellable(
        &self,
        wait: Duration,
        lease: Duration,
        cancellation: &Cancellation,
    ) -> Result<ConsulLease, Error> {
        self.acquire_with(Some(wait), Some(lease), Some(cancellation))
    }

    fn acquire_with(
        &self,
        wait: Option<Duration>,
        lease: Option<Duration>,
        cancellation: Option<&Cancellation>,
    ) -> Result<ConsulLease, Error> {
        let mut error = empty_error();
        let mut raw = ptr::null_mut();
        let status = unsafe {
            match (wait, lease, cancellation) {
                (None, None, None) => {
                    dilock_consul_acquire_default(self.raw.as_ptr(), &mut raw, &mut error)
                }
                (None, None, Some(token)) => dilock_consul_acquire_default_with_cancellation(
                    self.raw.as_ptr(),
                    token.raw.as_ptr(),
                    &mut raw,
                    &mut error,
                ),
                (Some(wait), Some(lease), None) => dilock_consul_acquire(
                    self.raw.as_ptr(),
                    duration_ms(wait)?,
                    duration_ms(lease)?,
                    &mut raw,
                    &mut error,
                ),
                (Some(wait), Some(lease), Some(token)) => {
                    dilock_consul_acquire_with_cancellation(
                        self.raw.as_ptr(),
                        duration_ms(wait)?,
                        duration_ms(lease)?,
                        token.raw.as_ptr(),
                        &mut raw,
                        &mut error,
                    )
                }
                _ => return Err(Error::InvalidArgument("wait and lease must be supplied together".into())),
            }
        };
        status_result(status, &error)?;
        Ok(ConsulLease {
            raw: NonNull::new(raw).ok_or_else(|| Error::Internal("lease handle was null".into()))?,
            _not_send_sync: PhantomData,
        })
    }

    /// Makes one immediate attempt; only confirmed contention returns `None`.
    pub fn try_acquire(&self, lease: Duration) -> Result<Option<ConsulLease>, Error> {
        let mut error = empty_error();
        let mut raw = ptr::null_mut();
        let status = unsafe {
            dilock_consul_try_acquire(
                self.raw.as_ptr(),
                duration_ms(lease)?,
                &mut raw,
                &mut error,
            )
        };
        if status == STATUS_BUSY {
            return Ok(None);
        }
        status_result(status, &error)?;
        Ok(Some(ConsulLease {
            raw: NonNull::new(raw).ok_or_else(|| Error::Internal("lease handle was null".into()))?,
            _not_send_sync: PhantomData,
        }))
    }
}

impl Drop for ConsulHandle {
    fn drop(&mut self) {
        unsafe { dilock_consul_lock_destroy(self.raw.as_ptr()) }
    }
}

/// One owning, non-cloneable session-backed Consul acquisition.
pub struct ConsulLease {
    raw: NonNull<CLease>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl ConsulLease {
    pub fn is_valid(&self) -> bool {
        unsafe { dilock_consul_lease_is_valid(self.raw.as_ptr()) != 0 }
    }

    pub fn effective_ttl(&self) -> Result<Duration, Error> {
        let mut error = empty_error();
        let mut ttl_ms = 0;
        let status = unsafe {
            dilock_consul_lease_effective_ttl_ms(self.raw.as_ptr(), &mut ttl_ms, &mut error)
        };
        status_result(status, &error)?;
        let millis = u64::try_from(ttl_ms)
            .map_err(|_| Error::Internal("native TTL was negative".into()))?;
        Ok(Duration::from_millis(millis))
    }

    pub fn fence(&self) -> Result<ConsulFence, Error> {
        let mut error = empty_error();
        let mut value = CConsulFence { lock_index: 0, coordination_epoch_bytes: 0 };
        let probe = unsafe {
            dilock_consul_lease_fence(self.raw.as_ptr(), &mut value, ptr::null_mut(), 0, &mut error)
        };
        if probe != STATUS_INVALID_ARGUMENT {
            status_result(probe, &error)?;
        }
        let capacity = value.coordination_epoch_bytes.checked_add(1)
            .ok_or_else(|| Error::Internal("epoch buffer size overflow".into()))?;
        let mut epoch = vec![0u8; capacity];
        let status = unsafe {
            dilock_consul_lease_fence(
                self.raw.as_ptr(),
                &mut value,
                epoch.as_mut_ptr().cast(),
                epoch.len(),
                &mut error,
            )
        };
        status_result(status, &error)?;
        epoch.truncate(value.coordination_epoch_bytes);
        let coordination_epoch = String::from_utf8(epoch)
            .map_err(|_| Error::Internal("native epoch was not valid UTF-8".into()))?;
        Ok(ConsulFence { coordination_epoch, lock_index: value.lock_index })
    }

    pub fn renew(&mut self) -> Result<bool, Error> {
        let mut error = empty_error();
        let mut renewed = 0;
        let status = unsafe {
            dilock_consul_lease_renew(self.raw.as_ptr(), &mut renewed, &mut error)
        };
        status_result(status, &error)?;
        Ok(renewed != 0)
    }

    pub fn release(&mut self) -> Result<ReleaseOutcome, Error> {
        let mut error = empty_error();
        let mut outcome = 2;
        let status = unsafe {
            dilock_consul_lease_release(self.raw.as_ptr(), &mut outcome, &mut error)
        };
        status_result(status, &error)?;
        release_result(outcome)
    }
}

impl Drop for ConsulLease {
    fn drop(&mut self) {
        unsafe { dilock_consul_lease_destroy(self.raw.as_ptr()) }
    }
}

/// Cancellation token for one potentially blocking acquisition call.
pub struct Cancellation {
    raw: NonNull<CCancellation>,
}

// The C object stores only an atomic cancellation flag, and callers borrow it
// for the entire acquisition. The C ABI contract requires it to outlive that
// operation, so sharing it across a cancelling thread is safe.
unsafe impl Send for Cancellation {}
unsafe impl Sync for Cancellation {}

impl Cancellation {
    pub fn new() -> Result<Self, Error> {
        check_abi()?;
        let mut error = empty_error();
        let raw = unsafe { dilock_cancellation_create(&mut error) };
        Ok(Self {
            raw: NonNull::new(raw)
                .ok_or_else(|| error_result(if error.code == 0 { 10 } else { error.code }, &error))?,
        })
    }

    pub fn cancel(&self) {
        unsafe { dilock_cancellation_request(self.raw.as_ptr()) }
    }
}

impl Drop for Cancellation {
    fn drop(&mut self) {
        unsafe { dilock_cancellation_destroy(self.raw.as_ptr()) }
    }
}

struct NativeOptions {
    value: CConsulOptions,
    _strings: [CString; 4],
}

fn native_options(options: &ConsulOptions) -> Result<NativeOptions, Error> {
    let endpoint = c_string(&options.endpoint)?;
    let namespace = c_string(&options.namespace)?;
    let epoch = c_string(&options.coordination_epoch)?;
    let token = c_string(&options.acl_token)?;
    let mut native = std::mem::MaybeUninit::<CConsulOptions>::uninit();
    unsafe { dilock_consul_options_init(native.as_mut_ptr()) };
    let mut native = unsafe { native.assume_init() };
    native.endpoint = endpoint.as_ptr();
    native.name_space = namespace.as_ptr();
    native.coordination_epoch = epoch.as_ptr();
    native.acl_token = token.as_ptr();
    native.rpc_timeout_ms = duration_ms(options.rpc_timeout)?;
    native.cleanup_timeout_ms = duration_ms(options.cleanup_timeout)?;
    native.poll_interval_ms = duration_ms(options.poll_interval)?;
    native.default_wait_ms = duration_ms(options.default_wait)?;
    native.default_lease_ms = duration_ms(options.default_lease)?;
    Ok(NativeOptions {
        value: native,
        _strings: [endpoint, namespace, epoch, token],
    })
}

fn check_abi() -> Result<(), Error> {
    if unsafe { dilock_c_abi_version() } == ABI_VERSION { Ok(()) } else { Err(Error::AbiMismatch) }
}

fn empty_error() -> CError {
    CError { code: 0, message: [0; ERROR_MESSAGE_CAPACITY] }
}

fn status_result(status: c_int, error: &CError) -> Result<(), Error> {
    if status == 0 { Ok(()) } else { Err(error_result(status, error)) }
}

fn error_result(status: c_int, error: &CError) -> Error {
    let bytes = unsafe { std::slice::from_raw_parts(error.message.as_ptr().cast::<u8>(), error.message.len()) };
    let message_len = bytes.iter().position(|byte| *byte == 0).unwrap_or(bytes.len());
    let message = String::from_utf8_lossy(&bytes[..message_len]).into_owned();
    match status {
        1 => Error::Busy,
        2 => Error::InvalidArgument(message),
        3 => Error::Cancelled,
        4 => Error::WaitTimeout,
        5 => Error::BackendUnavailable(message),
        6 => Error::AcquisitionUnknown,
        7 => Error::ProtocolMismatch(message),
        8 => Error::LeaseLost,
        9 => Error::UnsupportedCapability,
        _ => Error::Internal(message),
    }
}

fn release_result(value: i32) -> Result<ReleaseOutcome, Error> {
    match value {
        0 => Ok(ReleaseOutcome::Released),
        1 => Ok(ReleaseOutcome::AlreadyNotOwned),
        2 => Ok(ReleaseOutcome::Unknown),
        _ => Err(Error::Internal("unknown native release outcome".into())),
    }
}

fn c_string(value: &str) -> Result<CString, Error> {
    CString::new(value).map_err(|_| Error::InvalidArgument("strings cannot contain NUL".into()))
}

fn duration_ms(value: Duration) -> Result<i64, Error> {
    i64::try_from(value.as_millis())
        .map_err(|_| Error::InvalidArgument("duration exceeds the C ABI millisecond range".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_default_matches_c_abi_profile() {
        let options = ConsulOptions::default();
        assert_eq!(options.endpoint, "http://127.0.0.1:8500");
        assert_eq!(options.namespace, "default");
        assert_eq!(options.rpc_timeout, Duration::from_secs(2));
        assert_eq!(options.cleanup_timeout, Duration::from_secs(2));
        assert_eq!(options.poll_interval, Duration::from_millis(50));
        assert_eq!(options.default_wait, Duration::from_secs(5));
        assert_eq!(options.default_lease, Duration::from_secs(30));
    }

    #[test]
    fn embedded_nul_and_duration_overflow_fail_before_ffi() {
        assert!(c_string("bad\0resource").is_err());
        assert!(duration_ms(Duration::MAX).is_err());
    }

    #[test]
    fn maps_every_c_abi_status_and_release_value() {
        let error = empty_error();
        assert!(status_result(0, &error).is_ok());
        assert_eq!(error_result(1, &error), Error::Busy);
        assert_eq!(error_result(3, &error), Error::Cancelled);
        assert_eq!(error_result(4, &error), Error::WaitTimeout);
        assert_eq!(error_result(6, &error), Error::AcquisitionUnknown);
        assert_eq!(error_result(8, &error), Error::LeaseLost);
        assert_eq!(error_result(9, &error), Error::UnsupportedCapability);
        assert_eq!(release_result(0), Ok(ReleaseOutcome::Released));
        assert_eq!(release_result(1), Ok(ReleaseOutcome::AlreadyNotOwned));
        assert_eq!(release_result(2), Ok(ReleaseOutcome::Unknown));
        assert!(release_result(8).is_err());
    }
}
