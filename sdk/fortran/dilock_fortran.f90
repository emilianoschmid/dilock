module dilock_fortran
  !! Small ISO_C_BINDING facade over the versioned Dilock C ABI.
  !! Handles are opaque and must not be copied or used after their matching destroy call.
  use, intrinsic :: iso_c_binding, only: c_char, c_int, c_int32_t, c_int64_t, c_size_t, &
       c_null_char, c_null_ptr, c_ptr, c_funptr, c_loc, c_associated
  implicit none
  private

  integer, parameter, public :: dilock_ok = 0
  integer, parameter, public :: dilock_busy = 1
  integer, parameter, public :: dilock_invalid_argument = 2
  integer, parameter, public :: dilock_cancelled = 3
  integer, parameter, public :: dilock_wait_timeout = 4
  integer, parameter, public :: dilock_backend_unavailable = 5
  integer, parameter, public :: dilock_acquisition_unknown = 6
  integer, parameter, public :: dilock_protocol_mismatch = 7
  integer, parameter, public :: dilock_lease_lost = 8
  integer, parameter, public :: dilock_unsupported = 9
  integer, parameter, public :: dilock_internal = 10
  integer, parameter, public :: dilock_released = 0
  integer, parameter, public :: dilock_already_not_owned = 1
  integer, parameter, public :: dilock_release_unknown = 2
  integer, parameter :: error_message_capacity = 384

  type, bind(C) :: c_dilock_error
     integer(c_int32_t) :: code
     character(kind=c_char) :: message(error_message_capacity)
  end type

  type, bind(C) :: c_dilock_etcd_fence
     integer(c_int64_t) :: create_revision
     integer(c_size_t) :: coordination_epoch_bytes
  end type

  type, bind(C) :: c_dilock_consul_fence
     integer(c_int64_t) :: lock_index
     integer(c_size_t) :: coordination_epoch_bytes
  end type

  type, bind(C) :: c_dilock_redis_options
     type(c_ptr) :: host
     integer(c_int32_t) :: port
     type(c_ptr) :: username
     type(c_ptr) :: password
     integer(c_int32_t) :: database
     type(c_ptr) :: name_space
     type(c_ptr) :: coordination_epoch
     type(c_funptr) :: cleanup_callback
     type(c_ptr) :: cleanup_user_data
     integer(c_int64_t) :: rpc_timeout_ms
     integer(c_int64_t) :: cleanup_timeout_ms
     integer(c_int64_t) :: safety_margin_ms
     integer(c_int64_t) :: poll_interval_ms
     integer(c_int64_t) :: default_wait_ms
     integer(c_int64_t) :: default_lease_ms
  end type

  type, bind(C) :: c_dilock_etcd_options
     type(c_ptr) :: endpoint
     type(c_ptr) :: name_space
     type(c_ptr) :: coordination_epoch
     type(c_ptr) :: tls_root_certificates
     type(c_ptr) :: tls_client_certificate
     type(c_ptr) :: tls_client_private_key
     type(c_funptr) :: cleanup_callback
     type(c_ptr) :: cleanup_user_data
     integer(c_int64_t) :: rpc_timeout_ms
     integer(c_int64_t) :: cleanup_timeout_ms
     integer(c_int64_t) :: safety_margin_ms
     integer(c_int64_t) :: default_wait_ms
     integer(c_int64_t) :: default_lease_ms
     integer(c_int32_t) :: auto_renew
  end type

  type, bind(C) :: c_dilock_consul_options
     type(c_ptr) :: endpoint
     type(c_ptr) :: name_space
     type(c_ptr) :: coordination_epoch
     type(c_ptr) :: acl_token
     type(c_funptr) :: cleanup_callback
     type(c_ptr) :: cleanup_user_data
     integer(c_int64_t) :: rpc_timeout_ms
     integer(c_int64_t) :: cleanup_timeout_ms
     integer(c_int64_t) :: poll_interval_ms
     integer(c_int64_t) :: default_wait_ms
     integer(c_int64_t) :: default_lease_ms
  end type

  type, bind(C) :: c_dilock_postgres_options
     type(c_ptr) :: connection_string
     type(c_ptr) :: name_space
     type(c_ptr) :: coordination_epoch
     type(c_funptr) :: cleanup_callback
     type(c_ptr) :: cleanup_user_data
     integer(c_int64_t) :: rpc_timeout_ms
     integer(c_int64_t) :: cleanup_timeout_ms
     integer(c_int64_t) :: poll_interval_ms
     integer(c_int64_t) :: default_wait_ms
     integer(c_int64_t) :: default_lease_ms
  end type

  public :: redis_open, redis_lock, redis_acquire, redis_acquire_default
  public :: redis_try_acquire, redis_release
  public :: cancellation_create, cancellation_request, cancellation_destroy
  public :: redis_acquire_with_cancellation, redis_acquire_default_with_cancellation
  public :: redis_destroy_provider, redis_destroy_lock, redis_destroy_lease
  public :: etcd_open, etcd_lock, etcd_acquire, etcd_acquire_default, etcd_try_acquire
  public :: etcd_release, etcd_fence
  public :: etcd_acquire_with_cancellation, etcd_acquire_default_with_cancellation
  public :: etcd_destroy_provider, etcd_destroy_lock, etcd_destroy_lease
  public :: consul_open, consul_provision_namespace, consul_lock, consul_acquire
  public :: consul_acquire_default, consul_try_acquire, consul_release, consul_fence
  public :: consul_renew, consul_effective_ttl_ms, consul_acquire_with_cancellation
  public :: consul_acquire_default_with_cancellation
  public :: consul_destroy_provider, consul_destroy_lock, consul_destroy_lease
  public :: postgres_open, postgres_provision_namespace, postgres_lock, postgres_acquire
  public :: postgres_acquire_default, postgres_try_acquire, postgres_release
  public :: postgres_renew, postgres_is_valid, postgres_acquire_with_cancellation
  public :: postgres_acquire_default_with_cancellation
  public :: postgres_destroy_provider, postgres_destroy_lock, postgres_destroy_lease

  interface
     function c_cancellation_create(error) bind(C, name="dilock_cancellation_create") result(cancellation)
       import :: c_ptr, c_dilock_error
       type(c_dilock_error), intent(out) :: error
       type(c_ptr) :: cancellation
     end function
     subroutine c_cancellation_request(cancellation) bind(C, name="dilock_cancellation_request")
       import :: c_ptr
       type(c_ptr), value :: cancellation
     end subroutine
     subroutine c_cancellation_destroy(cancellation) bind(C, name="dilock_cancellation_destroy")
       import :: c_ptr
       type(c_ptr), value :: cancellation
     end subroutine

     subroutine c_redis_options_init(options) bind(C, name="dilock_redis_options_init")
       import :: c_dilock_redis_options
       type(c_dilock_redis_options), intent(out) :: options
     end subroutine
     integer(c_int) function c_redis_provider_create(options, provider, error) bind(C, name="dilock_redis_provider_create")
       import :: c_int, c_ptr, c_dilock_redis_options, c_dilock_error
       type(c_dilock_redis_options), intent(in) :: options
       type(c_ptr), intent(out) :: provider
       type(c_dilock_error), intent(out) :: error
     end function
     subroutine c_redis_provider_destroy(provider) bind(C, name="dilock_redis_provider_destroy")
       import :: c_ptr
       type(c_ptr), value :: provider
     end subroutine
     integer(c_int) function c_redis_lock_create(provider, resource, lock_handle, error) bind(C, name="dilock_redis_lock_create")
       import :: c_int, c_ptr, c_char, c_dilock_error
       type(c_ptr), value :: provider
       character(kind=c_char), intent(in) :: resource(*)
       type(c_ptr), intent(out) :: lock_handle
       type(c_dilock_error), intent(out) :: error
     end function
     subroutine c_redis_lock_destroy(lock_handle) bind(C, name="dilock_redis_lock_destroy")
       import :: c_ptr
       type(c_ptr), value :: lock_handle
     end subroutine
     integer(c_int) function c_redis_acquire(lock_handle, wait_ms, lease_ms, lease_handle, error) bind(C, name="dilock_redis_acquire")
       import :: c_int, c_int64_t, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle
       integer(c_int64_t), value :: wait_ms, lease_ms
       type(c_ptr), intent(out) :: lease_handle
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_redis_acquire_default(lock_handle, lease_handle, error) bind(C, name="dilock_redis_acquire_default")
       import :: c_int, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle
       type(c_ptr), intent(out) :: lease_handle
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_redis_acquire_with_cancellation(lock_handle, wait_ms, lease_ms, cancellation, lease_handle, error) &
          bind(C, name="dilock_redis_acquire_with_cancellation")
       import :: c_int, c_int64_t, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle, cancellation
       integer(c_int64_t), value :: wait_ms, lease_ms
       type(c_ptr) :: lease_handle
       type(c_dilock_error) :: error
     end function
     integer(c_int) function c_redis_acquire_default_with_cancellation(lock_handle, cancellation, lease_handle, error) &
          bind(C, name="dilock_redis_acquire_default_with_cancellation")
       import :: c_int, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle, cancellation
       type(c_ptr) :: lease_handle
       type(c_dilock_error) :: error
     end function
     integer(c_int) function c_redis_try_acquire(lock_handle, lease_ms, lease_handle, error) bind(C, name="dilock_redis_try_acquire")
       import :: c_int, c_int64_t, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle
       integer(c_int64_t), value :: lease_ms
       type(c_ptr), intent(out) :: lease_handle
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_redis_release(lease_handle, outcome, error) bind(C, name="dilock_redis_lease_release")
       import :: c_int, c_ptr, c_dilock_error
       type(c_ptr), value :: lease_handle
       integer(c_int), intent(inout) :: outcome
       type(c_dilock_error), intent(out) :: error
     end function
     subroutine c_redis_lease_destroy(lease_handle) bind(C, name="dilock_redis_lease_destroy")
       import :: c_ptr
       type(c_ptr), value :: lease_handle
     end subroutine

     subroutine c_etcd_options_init(options) bind(C, name="dilock_etcd_options_init")
       import :: c_dilock_etcd_options
       type(c_dilock_etcd_options), intent(out) :: options
     end subroutine
     subroutine c_consul_options_init(options) bind(C, name="dilock_consul_options_init")
       import :: c_dilock_consul_options
       type(c_dilock_consul_options), intent(out) :: options
     end subroutine
     integer(c_int) function c_consul_provision_namespace(options, error) bind(C, name="dilock_consul_provision_namespace")
       import :: c_int, c_dilock_consul_options, c_dilock_error
       type(c_dilock_consul_options), intent(in) :: options
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_consul_provider_create(options, provider, error) bind(C, name="dilock_consul_provider_create")
       import :: c_int, c_ptr, c_dilock_consul_options, c_dilock_error
       type(c_dilock_consul_options), intent(in) :: options
       type(c_ptr), intent(out) :: provider
       type(c_dilock_error), intent(out) :: error
     end function
     subroutine c_consul_provider_destroy(provider) bind(C, name="dilock_consul_provider_destroy")
       import :: c_ptr
       type(c_ptr), value :: provider
     end subroutine
     integer(c_int) function c_consul_lock_create(provider, resource, lock_handle, error) bind(C, name="dilock_consul_lock_create")
       import :: c_int, c_ptr, c_char, c_dilock_error
       type(c_ptr), value :: provider
       character(kind=c_char), intent(in) :: resource(*)
       type(c_ptr), intent(out) :: lock_handle
       type(c_dilock_error), intent(out) :: error
     end function
     subroutine c_consul_lock_destroy(lock_handle) bind(C, name="dilock_consul_lock_destroy")
       import :: c_ptr
       type(c_ptr), value :: lock_handle
     end subroutine
     integer(c_int) function c_consul_acquire(lock_handle, wait_ms, lease_ms, lease_handle, error) bind(C, name="dilock_consul_acquire")
       import :: c_int, c_int64_t, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle
       integer(c_int64_t), value :: wait_ms, lease_ms
       type(c_ptr), intent(out) :: lease_handle
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_consul_acquire_default(lock_handle, lease_handle, error) bind(C, name="dilock_consul_acquire_default")
       import :: c_int, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle
       type(c_ptr), intent(out) :: lease_handle
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_consul_acquire_with_cancellation(lock_handle, wait_ms, lease_ms, cancellation, lease_handle, error) &
          bind(C, name="dilock_consul_acquire_with_cancellation")
       import :: c_int, c_int64_t, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle, cancellation
       integer(c_int64_t), value :: wait_ms, lease_ms
       type(c_ptr), intent(out) :: lease_handle
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_consul_acquire_default_with_cancellation(lock_handle, cancellation, lease_handle, error) &
          bind(C, name="dilock_consul_acquire_default_with_cancellation")
       import :: c_int, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle, cancellation
       type(c_ptr), intent(out) :: lease_handle
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_consul_try_acquire(lock_handle, lease_ms, lease_handle, error) bind(C, name="dilock_consul_try_acquire")
       import :: c_int, c_int64_t, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle
       integer(c_int64_t), value :: lease_ms
       type(c_ptr), intent(out) :: lease_handle
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_consul_release(lease_handle, outcome, error) bind(C, name="dilock_consul_lease_release")
       import :: c_int, c_ptr, c_dilock_error
       type(c_ptr), value :: lease_handle
       integer(c_int), intent(inout) :: outcome
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_consul_fence(lease_handle, fence, epoch, epoch_capacity, error) bind(C, name="dilock_consul_lease_fence")
       import :: c_int, c_int64_t, c_size_t, c_ptr, c_char, c_dilock_consul_fence, c_dilock_error
       type(c_ptr), value :: lease_handle
       type(c_dilock_consul_fence), intent(out) :: fence
       character(kind=c_char), intent(out) :: epoch(*)
       integer(c_size_t), value :: epoch_capacity
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_consul_renew(lease_handle, renewed, error) bind(C, name="dilock_consul_lease_renew")
       import :: c_int, c_ptr, c_dilock_error
       type(c_ptr), value :: lease_handle
       integer(c_int), intent(out) :: renewed
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_consul_effective_ttl_ms(lease_handle, ttl_ms, error) bind(C, name="dilock_consul_lease_effective_ttl_ms")
       import :: c_int, c_int64_t, c_ptr, c_dilock_error
       type(c_ptr), value :: lease_handle
       integer(c_int64_t), intent(out) :: ttl_ms
       type(c_dilock_error), intent(out) :: error
     end function
     subroutine c_consul_lease_destroy(lease_handle) bind(C, name="dilock_consul_lease_destroy")
       import :: c_ptr
       type(c_ptr), value :: lease_handle
     end subroutine
     subroutine c_postgres_options_init(options) bind(C, name="dilock_postgres_options_init")
       import :: c_dilock_postgres_options
       type(c_dilock_postgres_options), intent(out) :: options
     end subroutine
     integer(c_int) function c_postgres_provision_namespace(options, error) &
          bind(C, name="dilock_postgres_provision_namespace")
       import :: c_int, c_dilock_postgres_options, c_dilock_error
       type(c_dilock_postgres_options), intent(in) :: options
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_postgres_provider_create(options, provider, error) &
          bind(C, name="dilock_postgres_provider_create")
       import :: c_int, c_ptr, c_dilock_postgres_options, c_dilock_error
       type(c_dilock_postgres_options), intent(in) :: options
       type(c_ptr), intent(out) :: provider
       type(c_dilock_error), intent(out) :: error
     end function
     subroutine c_postgres_provider_destroy(provider) bind(C, name="dilock_postgres_provider_destroy")
       import :: c_ptr
       type(c_ptr), value :: provider
     end subroutine
     integer(c_int) function c_postgres_lock_create(provider, resource, lock_handle, error) &
          bind(C, name="dilock_postgres_lock_create")
       import :: c_int, c_ptr, c_char, c_dilock_error
       type(c_ptr), value :: provider
       character(kind=c_char), intent(in) :: resource(*)
       type(c_ptr), intent(out) :: lock_handle
       type(c_dilock_error), intent(out) :: error
     end function
     subroutine c_postgres_lock_destroy(lock_handle) bind(C, name="dilock_postgres_lock_destroy")
       import :: c_ptr
       type(c_ptr), value :: lock_handle
     end subroutine
     integer(c_int) function c_postgres_acquire(lock_handle, wait_ms, lease_ms, lease_handle, error) &
          bind(C, name="dilock_postgres_acquire")
       import :: c_int, c_int64_t, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle
       integer(c_int64_t), value :: wait_ms, lease_ms
       type(c_ptr), intent(out) :: lease_handle
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_postgres_acquire_default(lock_handle, lease_handle, error) &
          bind(C, name="dilock_postgres_acquire_default")
       import :: c_int, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle
       type(c_ptr), intent(out) :: lease_handle
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_postgres_acquire_with_cancellation(lock_handle, wait_ms, lease_ms, cancellation, &
                                                                  lease_handle, error) &
          bind(C, name="dilock_postgres_acquire_with_cancellation")
       import :: c_int, c_int64_t, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle, cancellation
       integer(c_int64_t), value :: wait_ms, lease_ms
       type(c_ptr), intent(out) :: lease_handle
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_postgres_acquire_default_with_cancellation(lock_handle, cancellation, lease_handle, error) &
          bind(C, name="dilock_postgres_acquire_default_with_cancellation")
       import :: c_int, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle, cancellation
       type(c_ptr), intent(out) :: lease_handle
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_postgres_try_acquire(lock_handle, lease_handle, error) &
          bind(C, name="dilock_postgres_try_acquire")
       import :: c_int, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle
       type(c_ptr), intent(out) :: lease_handle
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_postgres_lease_is_valid(lease_handle) &
          bind(C, name="dilock_postgres_lease_is_valid")
       import :: c_int, c_ptr
       type(c_ptr), value :: lease_handle
     end function
     integer(c_int) function c_postgres_renew(lease_handle, renewed, error) &
          bind(C, name="dilock_postgres_lease_renew")
       import :: c_int, c_ptr, c_dilock_error
       type(c_ptr), value :: lease_handle
       integer(c_int), intent(out) :: renewed
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_postgres_release(lease_handle, outcome, error) &
          bind(C, name="dilock_postgres_lease_release")
       import :: c_int, c_ptr, c_dilock_error
       type(c_ptr), value :: lease_handle
       integer(c_int), intent(inout) :: outcome
       type(c_dilock_error), intent(out) :: error
     end function
     subroutine c_postgres_lease_destroy(lease_handle) bind(C, name="dilock_postgres_lease_destroy")
       import :: c_ptr
       type(c_ptr), value :: lease_handle
     end subroutine
     integer(c_int) function c_etcd_provider_create(options, provider, error) bind(C, name="dilock_etcd_provider_create")
       import :: c_int, c_ptr, c_dilock_etcd_options, c_dilock_error
       type(c_dilock_etcd_options), intent(in) :: options
       type(c_ptr), intent(out) :: provider
       type(c_dilock_error), intent(out) :: error
     end function
     subroutine c_etcd_provider_destroy(provider) bind(C, name="dilock_etcd_provider_destroy")
       import :: c_ptr
       type(c_ptr), value :: provider
     end subroutine
     integer(c_int) function c_etcd_lock_create(provider, resource, lock_handle, error) bind(C, name="dilock_etcd_lock_create")
       import :: c_int, c_ptr, c_char, c_dilock_error
       type(c_ptr), value :: provider
       character(kind=c_char), intent(in) :: resource(*)
       type(c_ptr), intent(out) :: lock_handle
       type(c_dilock_error), intent(out) :: error
     end function
     subroutine c_etcd_lock_destroy(lock_handle) bind(C, name="dilock_etcd_lock_destroy")
       import :: c_ptr
       type(c_ptr), value :: lock_handle
     end subroutine
     integer(c_int) function c_etcd_acquire(lock_handle, wait_ms, lease_ms, lease_handle, error) bind(C, name="dilock_etcd_acquire")
       import :: c_int, c_int64_t, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle
       integer(c_int64_t), value :: wait_ms, lease_ms
       type(c_ptr), intent(out) :: lease_handle
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_etcd_acquire_default(lock_handle, lease_handle, error) bind(C, name="dilock_etcd_acquire_default")
       import :: c_int, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle
       type(c_ptr), intent(out) :: lease_handle
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_etcd_acquire_with_cancellation(lock_handle, wait_ms, lease_ms, cancellation, lease_handle, error) &
          bind(C, name="dilock_etcd_acquire_with_cancellation")
       import :: c_int, c_int64_t, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle, cancellation
       integer(c_int64_t), value :: wait_ms, lease_ms
       type(c_ptr) :: lease_handle
       type(c_dilock_error) :: error
     end function
     integer(c_int) function c_etcd_acquire_default_with_cancellation(lock_handle, cancellation, lease_handle, error) &
          bind(C, name="dilock_etcd_acquire_default_with_cancellation")
       import :: c_int, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle, cancellation
       type(c_ptr) :: lease_handle
       type(c_dilock_error) :: error
     end function
     integer(c_int) function c_etcd_try_acquire(lock_handle, lease_ms, lease_handle, error) bind(C, name="dilock_etcd_try_acquire")
       import :: c_int, c_int64_t, c_ptr, c_dilock_error
       type(c_ptr), value :: lock_handle
       integer(c_int64_t), value :: lease_ms
       type(c_ptr), intent(out) :: lease_handle
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_etcd_release(lease_handle, outcome, error) bind(C, name="dilock_etcd_lease_release")
       import :: c_int, c_ptr, c_dilock_error
       type(c_ptr), value :: lease_handle
       integer(c_int), intent(inout) :: outcome
       type(c_dilock_error), intent(out) :: error
     end function
     integer(c_int) function c_etcd_fence(lease_handle, fence, epoch, epoch_capacity, error) bind(C, name="dilock_etcd_lease_fence")
       import :: c_int, c_size_t, c_ptr, c_char, c_dilock_etcd_fence, c_dilock_error
       type(c_ptr), value :: lease_handle
       type(c_dilock_etcd_fence), intent(out) :: fence
       character(kind=c_char), intent(out) :: epoch(*)
       integer(c_size_t), value :: epoch_capacity
       type(c_dilock_error), intent(out) :: error
     end function
     subroutine c_etcd_lease_destroy(lease_handle) bind(C, name="dilock_etcd_lease_destroy")
       import :: c_ptr
       type(c_ptr), value :: lease_handle
     end subroutine
  end interface

contains

  integer(c_int) function cancellation_create(cancellation, error_message) result(status)
    type(c_ptr), intent(inout) :: cancellation
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(cancellation)) then
       status = dilock_invalid_argument
       error_message = 'Output cancellation handle must be null; destroy its current handle before creating one.'
       return
    end if
    cancellation = c_cancellation_create(error)
    if (c_associated(cancellation)) then
       status = dilock_ok
       error_message = ''
    else
       status = int(error%code, c_int)
       if (status == dilock_ok) then
          status = dilock_internal
          error_message = 'C ABI returned a null cancellation handle without an error status.'
       else
          call copy_error(error, status, error_message)
       end if
    end if
  end function

  subroutine cancellation_request(cancellation)
    type(c_ptr), intent(in) :: cancellation
    call c_cancellation_request(cancellation)
  end subroutine

  subroutine cancellation_destroy(cancellation)
    type(c_ptr), intent(inout) :: cancellation
    if (.not. c_associated(cancellation)) return
    call c_cancellation_destroy(cancellation)
    cancellation = c_null_ptr
  end subroutine

  subroutine to_c_string(source, target, valid)
    character(len=*), intent(in) :: source
    character(kind=c_char), allocatable, target, intent(out) :: target(:)
    logical, intent(out) :: valid
    integer :: i, n
    ! Preserve the exact Fortran value. Trimming here would alias distinct
    ! Dilock resource names and silently change namespace/epoch bytes.
    n = len(source)
    allocate(target(n + 1))
    valid = .true.
    do i = 1, n
       if (iachar(source(i:i)) == 0) then
          valid = .false.
          target(1) = c_null_char
          return
       end if
       target(i) = achar(iachar(source(i:i)), kind=c_char)
    end do
    target(n + 1) = c_null_char
  end subroutine

  subroutine copy_error(native, status, error_message)
    type(c_dilock_error), intent(in) :: native
    integer(c_int), intent(inout) :: status
    character(len=*), intent(out) :: error_message
    integer :: i
    error_message = ''
    select case (status)
    case (dilock_ok, dilock_busy, dilock_invalid_argument, dilock_cancelled, dilock_wait_timeout, &
          dilock_backend_unavailable, dilock_acquisition_unknown, dilock_protocol_mismatch, &
          dilock_lease_lost, dilock_unsupported, dilock_internal)
       continue
    case default
       ! Fail closed if a newer or mismatched C ABI returns an unknown status.
       status = dilock_internal
       error_message = 'C ABI returned an unknown status code.'
       return
    end select
    if (status == dilock_ok) return
    do i = 1, min(len(error_message), error_message_capacity)
       if (native%message(i) == c_null_char) exit
       error_message(i:i) = native%message(i)
    end do
  end subroutine

  subroutine require_success_handle(status, handle, description, error_message)
    integer(c_int), intent(inout) :: status
    type(c_ptr), intent(in) :: handle
    character(len=*), intent(in) :: description
    character(len=*), intent(inout) :: error_message
    if (status == dilock_ok .and. .not. c_associated(handle)) then
       status = dilock_internal
       error_message = 'C ABI returned success without a ' // trim(description) // ' handle.'
    end if
  end subroutine

  subroutine normalize_release_outcome(outcome)
    integer(c_int), intent(inout) :: outcome
    select case (outcome)
    case (dilock_released, dilock_already_not_owned, dilock_release_unknown)
       continue
    case default
       ! An unrecognized ABI value is never evidence that cleanup succeeded.
       outcome = dilock_release_unknown
    end select
  end subroutine

  integer(c_int) function redis_open(epoch, provider, error_message, host, port, name_space) result(status)
    character(len=*), intent(in) :: epoch
    type(c_ptr), intent(inout) :: provider
    character(len=*), intent(out) :: error_message
    character(len=*), intent(in), optional :: host, name_space
    integer(c_int32_t), intent(in), optional :: port
    type(c_dilock_redis_options) :: options
    type(c_dilock_error) :: error
    character(kind=c_char), allocatable, target :: epoch_c(:), host_c(:), namespace_c(:)
    logical :: valid
    if (c_associated(provider)) then
       status = dilock_invalid_argument
       error_message = 'Output provider must be null; destroy its current handle before opening.'
       return
    end if
    call c_redis_options_init(options)
    call to_c_string(epoch, epoch_c, valid)
    if (.not. valid) then
       provider = c_null_ptr
       status = dilock_invalid_argument
       error_message = 'Epoch contains an embedded NUL byte.'
       return
    end if
    options%coordination_epoch = c_loc(epoch_c(1))
    if (present(host)) then
       call to_c_string(host, host_c, valid)
       if (.not. valid) then
          provider = c_null_ptr
          status = dilock_invalid_argument
          error_message = 'Redis host contains an embedded NUL byte.'
          return
       end if
       options%host = c_loc(host_c(1))
    end if
    if (present(port)) options%port = port
    if (present(name_space)) then
       call to_c_string(name_space, namespace_c, valid)
       if (.not. valid) then
          provider = c_null_ptr
          status = dilock_invalid_argument
          error_message = 'Namespace contains an embedded NUL byte.'
          return
       end if
       options%name_space = c_loc(namespace_c(1))
    end if
    status = c_redis_provider_create(options, provider, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, provider, 'Redis provider', error_message)
  end function

  integer(c_int) function redis_lock(provider, resource, lock_handle, error_message) result(status)
    type(c_ptr), intent(in) :: provider
    character(len=*), intent(in) :: resource
    type(c_ptr), intent(inout) :: lock_handle
    character(len=*), intent(out) :: error_message
    character(kind=c_char), allocatable, target :: resource_c(:)
    type(c_dilock_error) :: error
    logical :: valid
    if (c_associated(lock_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lock must be null; destroy its current handle before creating a lock.'
       return
    end if
    call to_c_string(resource, resource_c, valid)
    if (.not. valid) then
       lock_handle = c_null_ptr
       status = dilock_invalid_argument
       error_message = 'Resource contains an embedded NUL byte.'
       return
    end if
    status = c_redis_lock_create(provider, resource_c, lock_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lock_handle, 'Redis lock', error_message)
  end function

  integer(c_int) function redis_acquire(lock_handle, wait_ms, lease_ms, lease_handle, error_message) result(status)
    type(c_ptr), intent(in) :: lock_handle
    integer(c_int64_t), intent(in) :: wait_ms, lease_ms
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before acquiring.'
       return
    end if
    status = c_redis_acquire(lock_handle, wait_ms, lease_ms, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'Redis lease', error_message)
  end function

  integer(c_int) function redis_acquire_default(lock_handle, lease_handle, error_message) result(status)
    type(c_ptr), intent(in) :: lock_handle
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before acquiring.'
       return
    end if
    status = c_redis_acquire_default(lock_handle, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'Redis lease', error_message)
  end function

  integer(c_int) function redis_acquire_with_cancellation(lock_handle, wait_ms, lease_ms, cancellation, lease_handle, error_message) result(status)
    type(c_ptr), intent(in) :: lock_handle, cancellation
    integer(c_int64_t), intent(in) :: wait_ms, lease_ms
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before acquiring.'
       return
    end if
    if (.not. c_associated(cancellation)) then
       status = dilock_invalid_argument
       error_message = 'Cancellation handle must be non-null.'
       return
    end if
    status = c_redis_acquire_with_cancellation(lock_handle, wait_ms, lease_ms, cancellation, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'Redis lease', error_message)
  end function

  integer(c_int) function redis_acquire_default_with_cancellation(lock_handle, cancellation, lease_handle, error_message) result(status)
    type(c_ptr), intent(in) :: lock_handle, cancellation
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before acquiring.'
       return
    end if
    if (.not. c_associated(cancellation)) then
       status = dilock_invalid_argument
       error_message = 'Cancellation handle must be non-null.'
       return
    end if
    status = c_redis_acquire_default_with_cancellation(lock_handle, cancellation, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'Redis lease', error_message)
  end function

  integer(c_int) function redis_try_acquire(lock_handle, lease_ms, lease_handle, error_message) result(status)
    type(c_ptr), intent(in) :: lock_handle
    integer(c_int64_t), intent(in) :: lease_ms
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error

    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before try-acquire.'
       return
    end if

    status = c_redis_try_acquire(lock_handle, lease_ms, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'Redis lease', error_message)
  end function

  integer(c_int) function redis_release(lease_handle, outcome, error_message) result(status)
    type(c_ptr), intent(in) :: lease_handle
    integer(c_int), intent(out) :: outcome
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    integer(c_int) :: native_outcome
    native_outcome = dilock_release_unknown
    outcome = dilock_release_unknown
    status = c_redis_release(lease_handle, native_outcome, error)
    call copy_error(error, status, error_message)
    if (status /= dilock_ok) then
       outcome = dilock_release_unknown
    else
       outcome = native_outcome
       call normalize_release_outcome(outcome)
    end if
  end function

  subroutine redis_destroy_provider(provider)
    type(c_ptr), intent(inout) :: provider
    if (.not. c_associated(provider)) return
    call c_redis_provider_destroy(provider)
    provider = c_null_ptr
  end subroutine

  subroutine redis_destroy_lock(lock_handle)
    type(c_ptr), intent(inout) :: lock_handle
    if (.not. c_associated(lock_handle)) return
    call c_redis_lock_destroy(lock_handle)
    lock_handle = c_null_ptr
  end subroutine

  subroutine redis_destroy_lease(lease_handle)
    type(c_ptr), intent(inout) :: lease_handle
    if (.not. c_associated(lease_handle)) return
    call c_redis_lease_destroy(lease_handle)
    lease_handle = c_null_ptr
  end subroutine

  integer(c_int) function etcd_open(endpoint, epoch, provider, error_message, name_space, auto_renew) result(status)
    character(len=*), intent(in) :: endpoint, epoch
    type(c_ptr), intent(inout) :: provider
    character(len=*), intent(out) :: error_message
    character(len=*), intent(in), optional :: name_space
    logical, intent(in), optional :: auto_renew
    type(c_dilock_etcd_options) :: options
    type(c_dilock_error) :: error
    character(kind=c_char), allocatable, target :: endpoint_c(:), epoch_c(:), namespace_c(:)
    logical :: valid
    if (c_associated(provider)) then
       status = dilock_invalid_argument
       error_message = 'Output provider must be null; destroy its current handle before opening.'
       return
    end if
    call c_etcd_options_init(options)
    call to_c_string(endpoint, endpoint_c, valid)
    if (.not. valid) then
       provider = c_null_ptr
       status = dilock_invalid_argument
       error_message = 'etcd endpoint contains an embedded NUL byte.'
       return
    end if
    call to_c_string(epoch, epoch_c, valid)
    if (.not. valid) then
       provider = c_null_ptr
       status = dilock_invalid_argument
       error_message = 'Epoch contains an embedded NUL byte.'
       return
    end if
    options%endpoint = c_loc(endpoint_c(1))
    options%coordination_epoch = c_loc(epoch_c(1))
    if (present(name_space)) then
       call to_c_string(name_space, namespace_c, valid)
       if (.not. valid) then
          provider = c_null_ptr
          status = dilock_invalid_argument
          error_message = 'Namespace contains an embedded NUL byte.'
          return
       end if
       options%name_space = c_loc(namespace_c(1))
    end if
    if (present(auto_renew)) options%auto_renew = merge(1_c_int32_t, 0_c_int32_t, auto_renew)
    status = c_etcd_provider_create(options, provider, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, provider, 'etcd provider', error_message)
  end function

  integer(c_int) function etcd_lock(provider, resource, lock_handle, error_message) result(status)
    type(c_ptr), intent(in) :: provider
    character(len=*), intent(in) :: resource
    type(c_ptr), intent(inout) :: lock_handle
    character(len=*), intent(out) :: error_message
    character(kind=c_char), allocatable, target :: resource_c(:)
    type(c_dilock_error) :: error
    logical :: valid
    if (c_associated(lock_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lock must be null; destroy its current handle before creating a lock.'
       return
    end if
    call to_c_string(resource, resource_c, valid)
    if (.not. valid) then
       lock_handle = c_null_ptr
       status = dilock_invalid_argument
       error_message = 'Resource contains an embedded NUL byte.'
       return
    end if
    status = c_etcd_lock_create(provider, resource_c, lock_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lock_handle, 'etcd lock', error_message)
  end function

  integer(c_int) function etcd_acquire(lock_handle, wait_ms, lease_ms, lease_handle, error_message) result(status)
    type(c_ptr), intent(in) :: lock_handle
    integer(c_int64_t), intent(in) :: wait_ms, lease_ms
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before acquiring.'
       return
    end if
    status = c_etcd_acquire(lock_handle, wait_ms, lease_ms, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'etcd lease', error_message)
  end function

  integer(c_int) function etcd_acquire_default(lock_handle, lease_handle, error_message) result(status)
    type(c_ptr), intent(in) :: lock_handle
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before acquiring.'
       return
    end if
    status = c_etcd_acquire_default(lock_handle, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'etcd lease', error_message)
  end function

  integer(c_int) function etcd_acquire_with_cancellation(lock_handle, wait_ms, lease_ms, cancellation, lease_handle, error_message) result(status)
    type(c_ptr), intent(in) :: lock_handle, cancellation
    integer(c_int64_t), intent(in) :: wait_ms, lease_ms
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before acquiring.'
       return
    end if
    if (.not. c_associated(cancellation)) then
       status = dilock_invalid_argument
       error_message = 'Cancellation handle must be non-null.'
       return
    end if
    status = c_etcd_acquire_with_cancellation(lock_handle, wait_ms, lease_ms, cancellation, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'etcd lease', error_message)
  end function

  integer(c_int) function etcd_acquire_default_with_cancellation(lock_handle, cancellation, lease_handle, error_message) result(status)
    type(c_ptr), intent(in) :: lock_handle, cancellation
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before acquiring.'
       return
    end if
    if (.not. c_associated(cancellation)) then
       status = dilock_invalid_argument
       error_message = 'Cancellation handle must be non-null.'
       return
    end if
    status = c_etcd_acquire_default_with_cancellation(lock_handle, cancellation, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'etcd lease', error_message)
  end function

  integer(c_int) function etcd_try_acquire(lock_handle, lease_ms, lease_handle, error_message) result(status)
    !! etcd's native Lock RPC queues contenders and cannot provide nonblocking try semantics.
    !! The C ABI returns dilock_unsupported without network access; preserve that typed result.
    type(c_ptr), intent(in) :: lock_handle
    integer(c_int64_t), intent(in) :: lease_ms
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before try-acquire.'
       return
    end if
    status = c_etcd_try_acquire(lock_handle, lease_ms, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'etcd lease', error_message)
  end function

  integer(c_int) function etcd_release(lease_handle, outcome, error_message) result(status)
    type(c_ptr), intent(in) :: lease_handle
    integer(c_int), intent(out) :: outcome
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    integer(c_int) :: native_outcome
    native_outcome = dilock_release_unknown
    outcome = dilock_release_unknown
    status = c_etcd_release(lease_handle, native_outcome, error)
    call copy_error(error, status, error_message)
    if (status /= dilock_ok) then
       outcome = dilock_release_unknown
    else
       outcome = native_outcome
       call normalize_release_outcome(outcome)
    end if
  end function

  integer(c_int) function etcd_fence(lease_handle, revision, epoch, error_message) result(status)
    type(c_ptr), intent(in) :: lease_handle
    integer(c_int64_t), intent(out) :: revision
    character(len=*), intent(out) :: epoch, error_message
    type(c_dilock_etcd_fence) :: fence
    character(kind=c_char), allocatable, target :: epoch_c(:)
    type(c_dilock_error) :: error
    ! Do not expose an undefined revision when the C ABI rejects the request.
    revision = 0_c_int64_t
    ! The C ABI reports the epoch's byte length and requires one extra byte
    ! for its NUL terminator. Size this caller-owned buffer from the output
    ! argument instead of imposing an undocumented 1024-byte ceiling.
    allocate(epoch_c(len(epoch) + 1))
    status = c_etcd_fence(lease_handle, fence, epoch_c, int(size(epoch_c), c_size_t), error)
    call copy_error(error, status, error_message)
    epoch = ''
    if (status /= dilock_ok) return
    if (fence%coordination_epoch_bytes > int(len(epoch), c_size_t)) then
       status = dilock_invalid_argument
       error_message = 'Output epoch buffer is too small.'
       return
    end if
    block
      integer :: i
      do i = 1, int(fence%coordination_epoch_bytes)
         epoch(i:i) = epoch_c(i)
      end do
    end block
    revision = fence%create_revision
  end function

  subroutine etcd_destroy_provider(provider)
    type(c_ptr), intent(inout) :: provider
    if (.not. c_associated(provider)) return
    call c_etcd_provider_destroy(provider)
    provider = c_null_ptr
  end subroutine

  subroutine etcd_destroy_lock(lock_handle)
    type(c_ptr), intent(inout) :: lock_handle
    if (.not. c_associated(lock_handle)) return
    call c_etcd_lock_destroy(lock_handle)
    lock_handle = c_null_ptr
  end subroutine

  subroutine etcd_destroy_lease(lease_handle)
    type(c_ptr), intent(inout) :: lease_handle
    if (.not. c_associated(lease_handle)) return
    call c_etcd_lease_destroy(lease_handle)
    lease_handle = c_null_ptr
  end subroutine

  integer(c_int) function consul_open(endpoint, epoch, provider, error_message, name_space, acl_token) result(status)
    character(len=*), intent(in) :: endpoint, epoch
    type(c_ptr), intent(inout) :: provider
    character(len=*), intent(out) :: error_message
    character(len=*), intent(in), optional :: name_space, acl_token
    type(c_dilock_consul_options) :: options
    type(c_dilock_error) :: error
    character(kind=c_char), allocatable, target :: endpoint_c(:), epoch_c(:), namespace_c(:), token_c(:)
    logical :: valid
    if (c_associated(provider)) then
       status = dilock_invalid_argument
       error_message = 'Output provider must be null; destroy its current handle before opening.'
       return
    end if
    call c_consul_options_init(options)
    call to_c_string(endpoint, endpoint_c, valid)
    if (.not. valid) then
       provider = c_null_ptr
       status = dilock_invalid_argument
       error_message = 'Consul endpoint contains an embedded NUL byte.'
       return
    end if
    call to_c_string(epoch, epoch_c, valid)
    if (.not. valid) then
       provider = c_null_ptr
       status = dilock_invalid_argument
       error_message = 'Epoch contains an embedded NUL byte.'
       return
    end if
    options%endpoint = c_loc(endpoint_c(1))
    options%coordination_epoch = c_loc(epoch_c(1))
    if (present(name_space)) then
       call to_c_string(name_space, namespace_c, valid)
       if (.not. valid) then
          provider = c_null_ptr
          status = dilock_invalid_argument
          error_message = 'Namespace contains an embedded NUL byte.'
          return
       end if
       options%name_space = c_loc(namespace_c(1))
    end if
    if (present(acl_token)) then
       call to_c_string(acl_token, token_c, valid)
       if (.not. valid) then
          provider = c_null_ptr
          status = dilock_invalid_argument
          error_message = 'Consul ACL token contains an embedded NUL byte.'
          return
       end if
       options%acl_token = c_loc(token_c(1))
    end if
    status = c_consul_provider_create(options, provider, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, provider, 'Consul provider', error_message)
  end function

  integer(c_int) function consul_provision_namespace(endpoint, epoch, error_message, name_space, acl_token) result(status)
    character(len=*), intent(in) :: endpoint, epoch
    character(len=*), intent(out) :: error_message
    character(len=*), intent(in), optional :: name_space, acl_token
    type(c_dilock_consul_options) :: options
    type(c_dilock_error) :: error
    character(kind=c_char), allocatable, target :: endpoint_c(:), epoch_c(:), namespace_c(:), token_c(:)
    logical :: valid
    call c_consul_options_init(options)
    call to_c_string(endpoint, endpoint_c, valid)
    if (.not. valid) then
       status = dilock_invalid_argument
       error_message = 'Consul endpoint contains an embedded NUL byte.'
       return
    end if
    call to_c_string(epoch, epoch_c, valid)
    if (.not. valid) then
       status = dilock_invalid_argument
       error_message = 'Epoch contains an embedded NUL byte.'
       return
    end if
    options%endpoint = c_loc(endpoint_c(1))
    options%coordination_epoch = c_loc(epoch_c(1))
    if (present(name_space)) then
       call to_c_string(name_space, namespace_c, valid)
       if (.not. valid) then
          status = dilock_invalid_argument
          error_message = 'Namespace contains an embedded NUL byte.'
          return
       end if
       options%name_space = c_loc(namespace_c(1))
    end if
    if (present(acl_token)) then
       call to_c_string(acl_token, token_c, valid)
       if (.not. valid) then
          status = dilock_invalid_argument
          error_message = 'Consul ACL token contains an embedded NUL byte.'
          return
       end if
       options%acl_token = c_loc(token_c(1))
    end if
    status = c_consul_provision_namespace(options, error)
    call copy_error(error, status, error_message)
  end function

  integer(c_int) function consul_lock(provider, resource, lock_handle, error_message) result(status)
    type(c_ptr), intent(in) :: provider
    character(len=*), intent(in) :: resource
    type(c_ptr), intent(inout) :: lock_handle
    character(len=*), intent(out) :: error_message
    character(kind=c_char), allocatable, target :: resource_c(:)
    type(c_dilock_error) :: error
    logical :: valid
    if (c_associated(lock_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lock must be null; destroy its current handle before creating a lock.'
       return
    end if
    call to_c_string(resource, resource_c, valid)
    if (.not. valid) then
       lock_handle = c_null_ptr
       status = dilock_invalid_argument
       error_message = 'Resource contains an embedded NUL byte.'
       return
    end if
    status = c_consul_lock_create(provider, resource_c, lock_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lock_handle, 'Consul lock', error_message)
  end function

  integer(c_int) function consul_acquire(lock_handle, wait_ms, lease_ms, lease_handle, error_message) result(status)
    type(c_ptr), intent(in) :: lock_handle
    integer(c_int64_t), intent(in) :: wait_ms, lease_ms
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before acquiring.'
       return
    end if
    status = c_consul_acquire(lock_handle, wait_ms, lease_ms, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'Consul lease', error_message)
  end function

  integer(c_int) function consul_acquire_default(lock_handle, lease_handle, error_message) result(status)
    type(c_ptr), intent(in) :: lock_handle
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before acquiring.'
       return
    end if
    status = c_consul_acquire_default(lock_handle, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'Consul lease', error_message)
  end function

  integer(c_int) function consul_acquire_with_cancellation(lock_handle, wait_ms, lease_ms, cancellation, &
                                                           lease_handle, error_message) result(status)
    type(c_ptr), intent(in) :: lock_handle, cancellation
    integer(c_int64_t), intent(in) :: wait_ms, lease_ms
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before acquiring.'
       return
    end if
    if (.not. c_associated(cancellation)) then
       status = dilock_invalid_argument
       error_message = 'Cancellation handle must be non-null.'
       return
    end if
    status = c_consul_acquire_with_cancellation(lock_handle, wait_ms, lease_ms, cancellation, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'Consul lease', error_message)
  end function

  integer(c_int) function consul_acquire_default_with_cancellation(lock_handle, cancellation, &
                                                                   lease_handle, error_message) result(status)
    type(c_ptr), intent(in) :: lock_handle, cancellation
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before acquiring.'
       return
    end if
    if (.not. c_associated(cancellation)) then
       status = dilock_invalid_argument
       error_message = 'Cancellation handle must be non-null.'
       return
    end if
    status = c_consul_acquire_default_with_cancellation(lock_handle, cancellation, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'Consul lease', error_message)
  end function

  integer(c_int) function consul_try_acquire(lock_handle, lease_ms, lease_handle, error_message) result(status)
    type(c_ptr), intent(in) :: lock_handle
    integer(c_int64_t), intent(in) :: lease_ms
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before try-acquire.'
       return
    end if
    status = c_consul_try_acquire(lock_handle, lease_ms, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'Consul lease', error_message)
  end function

  integer(c_int) function consul_release(lease_handle, outcome, error_message) result(status)
    type(c_ptr), intent(in) :: lease_handle
    integer(c_int), intent(out) :: outcome
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    integer(c_int) :: native_outcome
    native_outcome = dilock_release_unknown
    outcome = dilock_release_unknown
    status = c_consul_release(lease_handle, native_outcome, error)
    call copy_error(error, status, error_message)
    if (status /= dilock_ok) then
       outcome = dilock_release_unknown
    else
       outcome = native_outcome
       call normalize_release_outcome(outcome)
    end if
  end function

  integer(c_int) function consul_fence(lease_handle, lock_index, epoch, error_message) result(status)
    type(c_ptr), intent(in) :: lease_handle
    integer(c_int64_t), intent(out) :: lock_index
    character(len=*), intent(out) :: epoch, error_message
    type(c_dilock_consul_fence) :: fence
    character(kind=c_char), allocatable, target :: epoch_c(:)
    type(c_dilock_error) :: error
    lock_index = 0_c_int64_t
    allocate(epoch_c(len(epoch) + 1))
    status = c_consul_fence(lease_handle, fence, epoch_c, int(size(epoch_c), c_size_t), error)
    call copy_error(error, status, error_message)
    epoch = ''
    if (status /= dilock_ok) return
    if (fence%coordination_epoch_bytes > int(len(epoch), c_size_t)) then
       status = dilock_invalid_argument
       error_message = 'Output epoch buffer is too small.'
       return
    end if
    block
      integer :: i
      do i = 1, int(fence%coordination_epoch_bytes)
         epoch(i:i) = epoch_c(i)
      end do
    end block
    lock_index = fence%lock_index
  end function

  integer(c_int) function consul_renew(lease_handle, renewed, error_message) result(status)
    type(c_ptr), intent(in) :: lease_handle
    integer(c_int), intent(out) :: renewed
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    renewed = 0_c_int
    status = c_consul_renew(lease_handle, renewed, error)
    call copy_error(error, status, error_message)
    if (status /= dilock_ok) renewed = 0_c_int
  end function

  integer(c_int) function consul_effective_ttl_ms(lease_handle, ttl_ms, error_message) result(status)
    type(c_ptr), intent(in) :: lease_handle
    integer(c_int64_t), intent(out) :: ttl_ms
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    ttl_ms = 0_c_int64_t
    status = c_consul_effective_ttl_ms(lease_handle, ttl_ms, error)
    call copy_error(error, status, error_message)
    if (status /= dilock_ok) ttl_ms = 0_c_int64_t
  end function

  subroutine consul_destroy_provider(provider)
    type(c_ptr), intent(inout) :: provider
    if (.not. c_associated(provider)) return
    call c_consul_provider_destroy(provider)
    provider = c_null_ptr
  end subroutine

  subroutine consul_destroy_lock(lock_handle)
    type(c_ptr), intent(inout) :: lock_handle
    if (.not. c_associated(lock_handle)) return
    call c_consul_lock_destroy(lock_handle)
    lock_handle = c_null_ptr
  end subroutine

  subroutine consul_destroy_lease(lease_handle)
    type(c_ptr), intent(inout) :: lease_handle
    if (.not. c_associated(lease_handle)) return
    call c_consul_lease_destroy(lease_handle)
    lease_handle = c_null_ptr
  end subroutine

  integer(c_int) function postgres_open(connection_string, epoch, provider, error_message, name_space) result(status)
    character(len=*), intent(in) :: connection_string, epoch
    type(c_ptr), intent(inout) :: provider
    character(len=*), intent(out) :: error_message
    character(len=*), intent(in), optional :: name_space
    type(c_dilock_postgres_options) :: options
    type(c_dilock_error) :: error
    character(kind=c_char), allocatable, target :: connection_c(:), epoch_c(:), namespace_c(:)
    logical :: valid
    if (c_associated(provider)) then
       status = dilock_invalid_argument
       error_message = 'Output provider must be null; destroy its current handle before opening.'
       return
    end if
    call c_postgres_options_init(options)
    call to_c_string(connection_string, connection_c, valid)
    if (.not. valid) then
       provider = c_null_ptr
       status = dilock_invalid_argument
       error_message = 'PostgreSQL connection string contains an embedded NUL byte.'
       return
    end if
    call to_c_string(epoch, epoch_c, valid)
    if (.not. valid) then
       provider = c_null_ptr
       status = dilock_invalid_argument
       error_message = 'Epoch contains an embedded NUL byte.'
       return
    end if
    options%connection_string = c_loc(connection_c(1))
    options%coordination_epoch = c_loc(epoch_c(1))
    if (present(name_space)) then
       call to_c_string(name_space, namespace_c, valid)
       if (.not. valid) then
          provider = c_null_ptr
          status = dilock_invalid_argument
          error_message = 'Namespace contains an embedded NUL byte.'
          return
       end if
       options%name_space = c_loc(namespace_c(1))
    end if
    status = c_postgres_provider_create(options, provider, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, provider, 'PostgreSQL provider', error_message)
  end function

  integer(c_int) function postgres_provision_namespace(connection_string, epoch, error_message, name_space) result(status)
    character(len=*), intent(in) :: connection_string, epoch
    character(len=*), intent(out) :: error_message
    character(len=*), intent(in), optional :: name_space
    type(c_dilock_postgres_options) :: options
    type(c_dilock_error) :: error
    character(kind=c_char), allocatable, target :: connection_c(:), epoch_c(:), namespace_c(:)
    logical :: valid
    call c_postgres_options_init(options)
    call to_c_string(connection_string, connection_c, valid)
    if (.not. valid) then
       status = dilock_invalid_argument
       error_message = 'PostgreSQL connection string contains an embedded NUL byte.'
       return
    end if
    call to_c_string(epoch, epoch_c, valid)
    if (.not. valid) then
       status = dilock_invalid_argument
       error_message = 'Epoch contains an embedded NUL byte.'
       return
    end if
    options%connection_string = c_loc(connection_c(1))
    options%coordination_epoch = c_loc(epoch_c(1))
    if (present(name_space)) then
       call to_c_string(name_space, namespace_c, valid)
       if (.not. valid) then
          status = dilock_invalid_argument
          error_message = 'Namespace contains an embedded NUL byte.'
          return
       end if
       options%name_space = c_loc(namespace_c(1))
    end if
    status = c_postgres_provision_namespace(options, error)
    call copy_error(error, status, error_message)
  end function

  integer(c_int) function postgres_lock(provider, resource, lock_handle, error_message) result(status)
    type(c_ptr), intent(in) :: provider
    character(len=*), intent(in) :: resource
    type(c_ptr), intent(inout) :: lock_handle
    character(len=*), intent(out) :: error_message
    character(kind=c_char), allocatable, target :: resource_c(:)
    type(c_dilock_error) :: error
    logical :: valid
    if (c_associated(lock_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lock must be null; destroy its current handle before creating a lock.'
       return
    end if
    call to_c_string(resource, resource_c, valid)
    if (.not. valid) then
       lock_handle = c_null_ptr
       status = dilock_invalid_argument
       error_message = 'Resource contains an embedded NUL byte.'
       return
    end if
    status = c_postgres_lock_create(provider, resource_c, lock_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lock_handle, 'PostgreSQL lock', error_message)
  end function

  integer(c_int) function postgres_acquire(lock_handle, wait_ms, lease_ms, lease_handle, error_message) result(status)
    type(c_ptr), intent(in) :: lock_handle
    integer(c_int64_t), intent(in) :: wait_ms, lease_ms
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before acquiring.'
       return
    end if
    status = c_postgres_acquire(lock_handle, wait_ms, lease_ms, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'PostgreSQL lease', error_message)
  end function

  integer(c_int) function postgres_acquire_default(lock_handle, lease_handle, error_message) result(status)
    type(c_ptr), intent(in) :: lock_handle
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before acquiring.'
       return
    end if
    status = c_postgres_acquire_default(lock_handle, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'PostgreSQL lease', error_message)
  end function

  integer(c_int) function postgres_acquire_with_cancellation(lock_handle, wait_ms, lease_ms, cancellation, &
                                                             lease_handle, error_message) result(status)
    type(c_ptr), intent(in) :: lock_handle, cancellation
    integer(c_int64_t), intent(in) :: wait_ms, lease_ms
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before acquiring.'
       return
    end if
    if (.not. c_associated(cancellation)) then
       status = dilock_invalid_argument
       error_message = 'Cancellation handle must be non-null.'
       return
    end if
    status = c_postgres_acquire_with_cancellation(lock_handle, wait_ms, lease_ms, cancellation, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'PostgreSQL lease', error_message)
  end function

  integer(c_int) function postgres_acquire_default_with_cancellation(lock_handle, cancellation, &
                                                                      lease_handle, error_message) result(status)
    type(c_ptr), intent(in) :: lock_handle, cancellation
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before acquiring.'
       return
    end if
    if (.not. c_associated(cancellation)) then
       status = dilock_invalid_argument
       error_message = 'Cancellation handle must be non-null.'
       return
    end if
    status = c_postgres_acquire_default_with_cancellation(lock_handle, cancellation, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'PostgreSQL lease', error_message)
  end function

  integer(c_int) function postgres_try_acquire(lock_handle, lease_handle, error_message) result(status)
    type(c_ptr), intent(in) :: lock_handle
    type(c_ptr), intent(inout) :: lease_handle
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    if (c_associated(lease_handle)) then
       status = dilock_invalid_argument
       error_message = 'Output lease must be null; destroy its current handle before try-acquire.'
       return
    end if
    status = c_postgres_try_acquire(lock_handle, lease_handle, error)
    call copy_error(error, status, error_message)
    call require_success_handle(status, lease_handle, 'PostgreSQL lease', error_message)
  end function

  integer(c_int) function postgres_is_valid(lease_handle) result(status)
    type(c_ptr), intent(in) :: lease_handle
    if (.not. c_associated(lease_handle)) then
       status = 0_c_int
    else
       status = c_postgres_lease_is_valid(lease_handle)
    end if
  end function

  integer(c_int) function postgres_renew(lease_handle, renewed, error_message) result(status)
    type(c_ptr), intent(in) :: lease_handle
    integer(c_int), intent(out) :: renewed
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    renewed = 0_c_int
    status = c_postgres_renew(lease_handle, renewed, error)
    call copy_error(error, status, error_message)
    if (status /= dilock_ok) renewed = 0_c_int
  end function

  integer(c_int) function postgres_release(lease_handle, outcome, error_message) result(status)
    type(c_ptr), intent(in) :: lease_handle
    integer(c_int), intent(out) :: outcome
    character(len=*), intent(out) :: error_message
    type(c_dilock_error) :: error
    integer(c_int) :: native_outcome
    native_outcome = dilock_release_unknown
    outcome = dilock_release_unknown
    status = c_postgres_release(lease_handle, native_outcome, error)
    call copy_error(error, status, error_message)
    if (status /= dilock_ok) then
       outcome = dilock_release_unknown
    else
       outcome = native_outcome
       call normalize_release_outcome(outcome)
    end if
  end function

  subroutine postgres_destroy_provider(provider)
    type(c_ptr), intent(inout) :: provider
    if (.not. c_associated(provider)) return
    call c_postgres_provider_destroy(provider)
    provider = c_null_ptr
  end subroutine

  subroutine postgres_destroy_lock(lock_handle)
    type(c_ptr), intent(inout) :: lock_handle
    if (.not. c_associated(lock_handle)) return
    call c_postgres_lock_destroy(lock_handle)
    lock_handle = c_null_ptr
  end subroutine

  subroutine postgres_destroy_lease(lease_handle)
    type(c_ptr), intent(inout) :: lease_handle
    if (.not. c_associated(lease_handle)) return
    call c_postgres_lease_destroy(lease_handle)
    lease_handle = c_null_ptr
  end subroutine
end module dilock_fortran
