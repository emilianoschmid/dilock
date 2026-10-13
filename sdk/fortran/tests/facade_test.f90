program facade_test
  use, intrinsic :: iso_c_binding, only: c_ptr, c_int, c_int64_t, c_associated, c_null_ptr
  use dilock_fortran
  implicit none

  type(c_ptr) :: provider = c_null_ptr, lock_handle = c_null_ptr, lease = c_null_ptr
  type(c_ptr) :: cancellation = c_null_ptr
  character(len=8) :: epoch
  character(len=1500) :: long_epoch, returned_epoch
  character(len=32) :: short_epoch
  character(len=384) :: message
  integer(c_int) :: status, outcome
  integer(c_int64_t) :: revision

  interface
    subroutine test_set_next_redis_lock_status(value) bind(C, name="dilock_test_set_next_redis_lock_status")
      import :: c_int
      integer(c_int), value :: value
    end subroutine
    subroutine test_set_next_redis_lock_success_without_output() &
        bind(C, name="dilock_test_set_next_redis_lock_success_without_output")
    end subroutine
    subroutine test_set_next_redis_release(status_value, outcome_value) bind(C, name="dilock_test_set_next_redis_release")
      import :: c_int
      integer(c_int), value :: status_value, outcome_value
    end subroutine
  end interface

  epoch = 'epoch  '
  status = redis_open(epoch, provider, message, host='redis  ', name_space='ns ')
  call require(status == dilock_ok, 'Redis provider accepts exact fixed-width values')
  block
    type(c_ptr) :: original_provider
    original_provider = provider
    status = redis_open(epoch, provider, message)
    call require(status == dilock_invalid_argument, 'Redis open rejects an already-owned output provider')
    call require(c_associated(provider, original_provider), 'Rejected Redis open preserves the existing provider')
  end block
  status = redis_lock(provider, 'job  ', lock_handle, message)
  call require(status == dilock_ok, 'Redis resource preserves trailing blanks')
  status = redis_acquire_default_with_cancellation(lock_handle, cancellation, lease, message)
  call require(status == dilock_invalid_argument .and. .not. c_associated(lease), &
                'Redis cancellable default acquire rejects a null cancellation handle')
  status = cancellation_create(cancellation, message)
  call require(status == dilock_ok .and. c_associated(cancellation), 'Cancellation handle creation succeeds')
  block
    type(c_ptr) :: original_cancellation
    original_cancellation = cancellation
    status = cancellation_create(cancellation, message)
    call require(status == dilock_invalid_argument, 'Cancellation creation rejects an already-owned output')
    call require(c_associated(cancellation, original_cancellation), 'Rejected cancellation creation preserves its handle')
  end block
  call cancellation_request(cancellation)
  status = redis_acquire_default_with_cancellation(lock_handle, cancellation, lease, message)
  call require(status == dilock_cancelled .and. .not. c_associated(lease), &
                'Redis cancellable default acquire preserves cancellation and leaves output null')
  call require(index(message, 'cancelled by test shim') > 0, 'Redis cancellation keeps the native diagnostic')
  call cancellation_destroy(cancellation)
  call require(.not. c_associated(cancellation), 'Cancellation destroy clears the caller handle')
  status = cancellation_create(cancellation, message)
  call require(status == dilock_ok, 'A fresh cancellation handle can be created')
  status = redis_acquire_with_cancellation(lock_handle, 100_c_int64_t, 1000_c_int64_t, &
                                            cancellation, lease, message)
  call require(status == dilock_ok .and. c_associated(lease), 'Redis cancellable explicit acquire succeeds')
  block
    type(c_ptr) :: original_lease
    original_lease = lease
    status = redis_acquire_default_with_cancellation(lock_handle, cancellation, lease, message)
    call require(status == dilock_invalid_argument, 'Redis cancellable acquire rejects an owned output lease')
    call require(c_associated(lease, original_lease), 'Rejected Redis cancellable acquire preserves its lease')
  end block
  status = redis_release(lease, outcome, message)
  call require(status == dilock_ok .and. outcome == dilock_released, 'Cancellable Redis lease releases normally')
  call redis_destroy_lease(lease)
  call cancellation_destroy(cancellation)
  block
    type(c_ptr) :: original_lock
    original_lock = lock_handle
    status = redis_lock(provider, 'job', lock_handle, message)
    call require(status == dilock_invalid_argument, 'Redis lock creation rejects an already-owned output handle')
    call require(c_associated(lock_handle, original_lock), 'Rejected Redis lock creation preserves the existing handle')
  end block
  status = redis_acquire_default(lock_handle, lease, message)
  call require(status == dilock_ok .and. c_associated(lease), &
                'Redis default acquisition returns a lease using provider defaults')
  block
    type(c_ptr) :: original_lease
    original_lease = lease
    status = redis_acquire(lock_handle, 10_c_int64_t, 1000_c_int64_t, lease, message)
    call require(status == dilock_invalid_argument, 'Redis acquire rejects an already-owned output lease')
    call require(c_associated(lease, original_lease), 'Rejected Redis acquire preserves the existing lease')
    status = redis_acquire_default(lock_handle, lease, message)
    call require(status == dilock_invalid_argument, 'Redis default acquire rejects an already-owned output lease')
    call require(c_associated(lease, original_lease), 'Rejected Redis default acquire preserves the existing lease')
    status = redis_try_acquire(lock_handle, 1000_c_int64_t, lease, message)
    call require(status == dilock_invalid_argument, &
                 'Redis try-acquire rejects an already-owned output handle')
    call require(c_associated(lease, original_lease), &
                 'Rejected Redis try-acquire preserves the existing lease handle')
    status = redis_release(lease, outcome, message)
    call require(status == dilock_ok .and. outcome == dilock_released, &
                 'Preserved original lease remains releasable after rejected try-acquire')
  end block
  call redis_destroy_lease(lease)
  call redis_destroy_lock(lock_handle)

  call test_set_next_redis_lock_success_without_output()
  status = redis_lock(provider, 'job  ', lock_handle, message)
  call require(status == dilock_internal, 'A success status without an output handle fails closed')
  call require(.not. c_associated(lock_handle), 'A missing success handle remains null')
  call require(index(message, 'without a Redis lock handle') > 0, &
               'A missing success handle receives a diagnostic')

  call test_set_next_redis_lock_status(123_c_int)
  status = redis_lock(provider, 'job  ', lock_handle, message)
  call require(status == dilock_internal, 'Unknown C ABI status maps to the internal error status')
  call require(index(message, 'unknown status code') > 0, 'Unknown C ABI status receives a safe diagnostic')
  call require(.not. c_associated(lock_handle), 'Unknown C ABI status leaves the lock output null')
  status = redis_lock(provider, 'job  ', lock_handle, message)
  call require(status == dilock_ok, 'Redis lock can be created after an unknown status')
  status = redis_acquire_default(lock_handle, lease, message)
  call require(status == dilock_ok, 'Redis lease is available for outcome normalization')
  call test_set_next_redis_release(0_c_int, dilock_already_not_owned)
  status = redis_release(lease, outcome, message)
  call require(status == dilock_ok .and. outcome == dilock_already_not_owned .and. &
                outcome /= dilock_released, 'Already-not-owned release remains distinct from successful release')
  call redis_destroy_lease(lease)

  status = redis_acquire_default(lock_handle, lease, message)
  call require(status == dilock_ok, 'Redis lease is available for unknown release outcome')
  call test_set_next_redis_release(0_c_int, 123_c_int)
  status = redis_release(lease, outcome, message)
  call require(status == dilock_ok .and. outcome == dilock_release_unknown, &
                'Unknown C ABI release outcome maps to uncertain cleanup')
  call redis_destroy_lease(lease)

  status = redis_acquire_default(lock_handle, lease, message)
  call require(status == dilock_ok, 'Redis lease is available for unknown release status')
  call test_set_next_redis_release(123_c_int, dilock_released)
  status = redis_release(lease, outcome, message)
  call require(status == dilock_internal .and. outcome == dilock_release_unknown, &
                'Unknown C ABI release status maps to internal and uncertain cleanup')
  call require(index(message, 'unknown status code') > 0, 'Unknown release status receives a safe diagnostic')
  call redis_destroy_lease(lease)
  call redis_destroy_lock(lock_handle)

  status = redis_lock(provider, 'bad' // achar(0) // 'resource', lock_handle, message)
  call require(status == dilock_invalid_argument, 'Redis resource rejects embedded NUL')
  call require(.not. c_associated(lock_handle), 'Rejected resource returns a null handle')
  call redis_destroy_provider(provider)

  status = redis_open('bad' // achar(0) // 'epoch', provider, message)
  call require(status == dilock_invalid_argument, 'Redis epoch rejects embedded NUL')
  call require(.not. c_associated(provider), 'Rejected epoch returns a null provider')

  long_epoch = repeat('E', len(long_epoch))
  status = etcd_open('etcd:2379', long_epoch, provider, message)
  call require(status == dilock_ok, 'etcd provider accepts long coordination epoch')
  block
    type(c_ptr) :: original_provider
    original_provider = provider
    status = etcd_open('etcd:2379', long_epoch, provider, message)
    call require(status == dilock_invalid_argument, 'etcd open rejects an already-owned output provider')
    call require(c_associated(provider, original_provider), 'Rejected etcd open preserves the existing provider')
  end block
  status = etcd_lock(provider, 'reports/daily', lock_handle, message)
  call require(status == dilock_ok, 'etcd resource creation succeeds')
  status = cancellation_create(cancellation, message)
  call require(status == dilock_ok, 'etcd cancellation handle creation succeeds')
  status = etcd_acquire_default_with_cancellation(lock_handle, cancellation, lease, message)
  call require(status == dilock_ok .and. c_associated(lease), 'etcd cancellable default acquire succeeds')
  block
    type(c_ptr) :: original_lease
    original_lease = lease
    status = etcd_acquire_with_cancellation(lock_handle, 100_c_int64_t, 1000_c_int64_t, &
                                             cancellation, lease, message)
    call require(status == dilock_invalid_argument, 'etcd cancellable acquire rejects an owned output lease')
    call require(c_associated(lease, original_lease), 'Rejected etcd cancellable acquire preserves its lease')
  end block
  status = etcd_release(lease, outcome, message)
  call require(status == dilock_ok .and. outcome == dilock_released, 'Cancellable etcd lease releases normally')
  call etcd_destroy_lease(lease)
  call cancellation_destroy(cancellation)
  status = cancellation_create(cancellation, message)
  call require(status == dilock_ok, 'Fresh etcd cancellation handle creation succeeds')
  call cancellation_request(cancellation)
  status = etcd_acquire_with_cancellation(lock_handle, 100_c_int64_t, 1000_c_int64_t, &
                                           cancellation, lease, message)
  call require(status == dilock_cancelled .and. .not. c_associated(lease), &
                'etcd cancellable explicit acquire preserves cancellation and leaves output null')
  call cancellation_destroy(cancellation)
  block
    type(c_ptr) :: original_lock
    original_lock = lock_handle
    status = etcd_lock(provider, 'other', lock_handle, message)
    call require(status == dilock_invalid_argument, 'etcd lock creation rejects an already-owned output handle')
    call require(c_associated(lock_handle, original_lock), 'Rejected etcd lock creation preserves the existing handle')
  end block
  status = etcd_acquire(lock_handle, 1_c_int64_t, 30000_c_int64_t, lease, message)
  call require(status == dilock_ok, 'etcd acquisition shim succeeds')
  call etcd_destroy_lease(lease)
  status = etcd_acquire_default(lock_handle, lease, message)
  call require(status == dilock_ok .and. c_associated(lease), &
                'etcd default acquisition returns a lease using provider defaults')
  block
    type(c_ptr) :: original_lease
    original_lease = lease
    status = etcd_try_acquire(lock_handle, 30000_c_int64_t, lease, message)
    call require(status == dilock_invalid_argument, 'etcd try-acquire rejects an already-owned output lease')
    call require(c_associated(lease, original_lease), 'Rejected etcd try-acquire preserves the existing lease')
  end block
  call etcd_destroy_lease(lease)
  status = etcd_try_acquire(lock_handle, 30000_c_int64_t, lease, message)
  call require(status == dilock_unsupported, 'etcd try-acquire exposes native Lock capability limits')
  call require(.not. c_associated(lease), 'Unsupported etcd try-acquire leaves the output lease null')
  call require(index(message, 'no safe try-acquire') > 0, 'Unsupported etcd try-acquire preserves its diagnostic')
  block
    type(c_ptr) :: original_lease
    status = etcd_acquire(lock_handle, 10_c_int64_t, 1000_c_int64_t, lease, message)
    call require(status == dilock_ok, 'etcd acquire succeeds after unsupported try-acquire')
    original_lease = lease
    status = etcd_acquire(lock_handle, 10_c_int64_t, 1000_c_int64_t, lease, message)
    call require(status == dilock_invalid_argument, 'etcd acquire rejects an already-owned output lease')
    call require(c_associated(lease, original_lease), 'Rejected etcd acquire preserves the existing lease')
    status = etcd_acquire_default(lock_handle, lease, message)
    call require(status == dilock_invalid_argument, 'etcd default acquire rejects an already-owned output lease')
    call require(c_associated(lease, original_lease), 'Rejected etcd default acquire preserves the existing lease')
  end block

  returned_epoch = ''
  status = etcd_fence(lease, revision, returned_epoch, message)
  call require(status == dilock_ok, 'etcd fence supports caller buffers over 1024 bytes')
  call require(revision == 42_c_int64_t, 'etcd fence returns create revision')
  call require(returned_epoch == long_epoch, 'etcd fence preserves all epoch bytes')
  short_epoch = 'must be cleared'
  status = etcd_fence(lease, revision, short_epoch, message)
  call require(status == dilock_invalid_argument, 'etcd fence rejects an undersized caller buffer')
  call require(short_epoch == '', 'Failed etcd fence clears the epoch output')
  call require(revision == 0_c_int64_t, 'Failed etcd fence clears the revision output')

  status = etcd_release(lease, outcome, message)
  call require(status == dilock_ok .and. outcome == dilock_released, 'etcd release returns its outcome')
  call etcd_destroy_lease(lease)
  call etcd_destroy_lock(lock_handle)
  call etcd_destroy_provider(provider)
contains
  subroutine require(condition, description)
    logical, intent(in) :: condition
    character(len=*), intent(in) :: description
    if (.not. condition) then
      write(*, '(a)') 'FAIL: ' // description
      error stop 1
    end if
    write(*, '(a)') 'PASS: ' // description
  end subroutine
end program facade_test
