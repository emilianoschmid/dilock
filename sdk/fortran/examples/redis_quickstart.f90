program redis_quickstart
  use, intrinsic :: iso_c_binding, only: c_ptr, c_int, c_int64_t, c_null_ptr
  use dilock_fortran
  implicit none
  type(c_ptr) :: provider = c_null_ptr, resource_lock = c_null_ptr, lease = c_null_ptr
  character(len=384) :: message
  integer(c_int) :: status, outcome = dilock_release_unknown

  status = redis_open('local-dev-1', provider, message)
  if (status /= dilock_ok) error stop 'provider creation failed'
  status = redis_lock(provider, 'reports/daily', resource_lock, message)
  if (status /= dilock_ok) error stop 'resource handle creation failed'
  status = redis_acquire(resource_lock, 5000_c_int64_t, 30000_c_int64_t, lease, message)
  if (status /= dilock_ok) error stop 'acquisition failed'

  ! Keep work short; application writes still need their own stale-owner guard.
  status = redis_release(lease, outcome, message)
  if (status /= dilock_ok .or. outcome /= dilock_released) then
    if (outcome == dilock_already_not_owned) then
      write(*, '(a)') 'lease is no longer owned; protected work must be treated as stale'
    else
      write(*, '(a,i0)') 'lease release was not confirmed; release outcome=', outcome
    end if
    write(*, '(a)') trim(message)
    call redis_destroy_lease(lease)
    call redis_destroy_lock(resource_lock)
    call redis_destroy_provider(provider)
    error stop 'Redis quickstart failed'
  end if
  call redis_destroy_lease(lease)
  call redis_destroy_lock(resource_lock)
  call redis_destroy_provider(provider)
end program
