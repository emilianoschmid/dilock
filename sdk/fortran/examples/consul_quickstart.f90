program consul_quickstart
  use, intrinsic :: iso_c_binding, only: c_ptr, c_int, c_int64_t, c_null_ptr
  use dilock_fortran
  implicit none

  type(c_ptr) :: provider = c_null_ptr, lock_handle = c_null_ptr, lease = c_null_ptr
  character(len=384) :: message, fence_epoch
  integer(c_int) :: status, outcome, renewed
  integer(c_int64_t) :: lock_index, ttl_ms

  ! The namespace metadata must already be provisioned for this epoch.
  status = consul_open('http://consul:8500', 'local-consul-epoch', provider, message, &
                       name_space='billing')
  if (status /= dilock_ok) then
    write(*, '(a)') trim(message)
    error stop 'Consul provider creation failed'
  end if
  status = consul_lock(provider, 'invoice/42/reconcile', lock_handle, message)
  if (status /= dilock_ok) then
    write(*, '(a)') trim(message)
    goto 900
  end if
  status = consul_acquire_default(lock_handle, lease, message)
  if (status /= dilock_ok) then
    write(*, '(a)') trim(message)
    goto 900
  end if
  status = consul_fence(lease, lock_index, fence_epoch, message)
  if (status /= dilock_ok) then
    write(*, '(a)') trim(message)
    goto 900
  end if
  write(*, '(a,1x,a,1x,i0)') 'Consul fencing token:', trim(fence_epoch), lock_index
  status = consul_effective_ttl_ms(lease, ttl_ms, message)
  if (status /= dilock_ok) goto 900
  status = consul_renew(lease, renewed, message)
  if (status /= dilock_ok .or. renewed == 0) goto 900

  status = consul_release(lease, outcome, message)
  if (status /= dilock_ok .or. outcome /= dilock_released) then
    write(*, '(a)') trim(message)
    goto 900
  end if

900 continue
  call consul_destroy_lease(lease)
  call consul_destroy_lock(lock_handle)
  call consul_destroy_provider(provider)
  if (status /= dilock_ok) error stop 'Consul operation did not complete successfully'
end program consul_quickstart
