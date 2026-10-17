program consul_api_consumer
  use, intrinsic :: iso_c_binding, only: c_ptr, c_int, c_int64_t, c_null_ptr
  use dilock_fortran
  implicit none
  type(c_ptr) :: provider = c_null_ptr, lock_handle = c_null_ptr, lease = c_null_ptr
  character(len=384) :: message, epoch
  integer(c_int) :: status, outcome, renewed
  integer(c_int64_t) :: lock_index, ttl_ms

  ! Compile/link the installed public Consul facade without requiring a server.
  if (command_argument_count() > 0) then
    status = consul_open('http://127.0.0.1:8500', 'consumer-epoch', provider, message)
    if (status /= dilock_ok) error stop 'Consul provider creation failed'
    status = consul_lock(provider, 'artifact-consumer/resource', lock_handle, message)
    if (status /= dilock_ok) error stop 'Consul lock creation failed'
    status = consul_acquire_default(lock_handle, lease, message)
    if (status /= dilock_ok) error stop 'Consul acquisition failed'
    status = consul_fence(lease, lock_index, epoch, message)
    if (status /= dilock_ok) error stop 'Consul fence retrieval failed'
    status = consul_effective_ttl_ms(lease, ttl_ms, message)
    if (status /= dilock_ok) error stop 'Consul TTL retrieval failed'
    status = consul_renew(lease, renewed, message)
    if (status /= dilock_ok) error stop 'Consul renewal failed'
    status = consul_release(lease, outcome, message)
    call consul_destroy_lease(lease)
    call consul_destroy_lock(lock_handle)
    call consul_destroy_provider(provider)
    if (status /= dilock_ok) error stop 'Consul release failed'
  end if
end program consul_api_consumer
