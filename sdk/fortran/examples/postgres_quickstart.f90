program postgres_example
  use, intrinsic :: iso_c_binding, only: c_ptr, c_int, c_int64_t, c_null_ptr
  use dilock_fortran
  implicit none

  type(c_ptr) :: provider = c_null_ptr, resource_lock = c_null_ptr, lease = c_null_ptr
  character(len=384) :: message
  integer(c_int) :: status, outcome

  ! Provisioning is explicit; provider construction only validates metadata.
  status = postgres_provision_namespace('postgresql://postgres:postgres@127.0.0.1:5432/dilock', &
                                        'fortran-postgres-demo-epoch', message, name_space='fortran-demo')
  if (status /= dilock_ok) error stop 'namespace provisioning failed'

  status = postgres_open('postgresql://postgres:postgres@127.0.0.1:5432/dilock', &
                         'fortran-postgres-demo-epoch', provider, message, name_space='fortran-demo')
  if (status /= dilock_ok) error stop 'provider creation failed'
  status = postgres_lock(provider, 'reports/daily', resource_lock, message)
  if (status /= dilock_ok) then
    call postgres_destroy_provider(provider)
    error stop 'resource handle creation failed'
  end if
  status = postgres_acquire(resource_lock, 5000_c_int64_t, 30000_c_int64_t, lease, message)
  if (status /= dilock_ok) then
    call postgres_destroy_lock(resource_lock)
    call postgres_destroy_provider(provider)
    error stop 'acquisition failed'
  end if

  status = postgres_release(lease, outcome, message)
  if (status /= dilock_ok .or. outcome /= dilock_released) then
    call postgres_destroy_lease(lease)
    call postgres_destroy_lock(resource_lock)
    call postgres_destroy_provider(provider)
    error stop 'release was not confirmed'
  end if
  call postgres_destroy_lease(lease)
  call postgres_destroy_lock(resource_lock)
  call postgres_destroy_provider(provider)
end program
