program etcd_configured_quickstart
  use, intrinsic :: iso_c_binding, only: c_ptr, c_int, c_int64_t, c_null_ptr, c_associated
  use dilock_fortran
  implicit none

  type(c_ptr) :: provider = c_null_ptr
  type(c_ptr) :: resource_lock = c_null_ptr
  type(c_ptr) :: lease = c_null_ptr
  character(len=384) :: message, fence_epoch
  integer(c_int) :: status, outcome = dilock_release_unknown
  integer(c_int64_t) :: fence_revision

  ! The endpoint and epoch are explicit. The provider is reused for the lock.
  status = etcd_open('etcd:2379', 'prod-east-1', provider, message, &
                     name_space='reports', auto_renew=.true.)
  if (status /= dilock_ok) then
    write(*, '(a)') trim(message)
    error stop 'etcd provider creation failed'
  end if

  status = etcd_lock(provider, 'daily', resource_lock, message)
  if (status /= dilock_ok) then
    write(*, '(a)') trim(message)
    goto 900
  end if

  ! Explicit 10-second acquisition wait and 60-second requested lease.
  status = etcd_acquire(resource_lock, 10000_c_int64_t, 60000_c_int64_t, lease, message)
  if (status /= dilock_ok) then
    write(*, '(a)') trim(message)
    goto 900
  end if

  status = etcd_fence(lease, fence_revision, fence_epoch, message)
  if (status /= dilock_ok) then
    write(*, '(a)') trim(message)
    goto 900
  end if
  write(*, '(a,1x,a,1x,i0)') 'daily fencing token:', trim(fence_epoch), fence_revision
  ! The protected resource must atomically reject stale (epoch, revision) tokens.

  status = etcd_release(lease, outcome, message)
  if (status /= dilock_ok .or. outcome /= dilock_released) then
    if (outcome == dilock_already_not_owned) then
      write(*, '(a)') 'daily lease is no longer owned; protected work must be treated as stale'
    else
      write(*, '(a,i0)') 'daily lease release was not confirmed; release outcome=', outcome
    end if
    write(*, '(a)') trim(message)
    goto 900
  end if

900 continue
  ! Opaque handles are destroyed exactly once in reverse ownership order.
  if (c_associated(lease)) call etcd_destroy_lease(lease)
  if (c_associated(resource_lock)) call etcd_destroy_lock(resource_lock)
  if (c_associated(provider)) call etcd_destroy_provider(provider)
  if (status /= dilock_ok .or. outcome /= dilock_released) error stop 'configured etcd quickstart failed'
end program
