program live_backend_consumer
  use, intrinsic :: iso_c_binding, only: c_ptr, c_int, c_int32_t, c_int64_t, c_null_ptr, c_associated
  use dilock_fortran
  implicit none

  character(len=32) :: backend
  character(len=512) :: endpoint, namespace, epoch, port_text
  character(len=512) :: fence_epoch
  character(len=384) :: message
  type(c_ptr) :: provider_a = c_null_ptr, provider_b = c_null_ptr, provider_c = c_null_ptr
  type(c_ptr) :: lock_a = c_null_ptr, lock_b = c_null_ptr, lock_c = c_null_ptr
  type(c_ptr) :: lease_a = c_null_ptr, lease_b = c_null_ptr, lease_c = c_null_ptr
  integer(c_int) :: status, outcome
  integer(c_int64_t) :: revision_a, revision_b
  integer(c_int32_t) :: port
  integer :: checks

  checks = 0
  call get_command_argument(1, backend)
  if (trim(backend) == 'redis') then
    call get_command_argument(2, endpoint)
    call get_command_argument(3, port_text)
    call get_command_argument(4, namespace)
    call get_command_argument(5, epoch)
    read(port_text, *) port
    call run_redis()
  else if (trim(backend) == 'etcd') then
    call get_command_argument(2, endpoint)
    call get_command_argument(3, namespace)
    call get_command_argument(4, epoch)
    call run_etcd()
  else
    error stop 'usage: dilock_fortran_live_consumer redis HOST PORT NAMESPACE EPOCH | etcd ENDPOINT NAMESPACE EPOCH'
  end if
  write(*, '(a,i0)') 'LIVE CONSUMER CHECKS PASSED: ', checks

contains

  subroutine require(condition, description)
    logical, intent(in) :: condition
    character(len=*), intent(in) :: description
    if (.not. condition) then
      write(*, '(a)') 'FAIL: ' // description
      error stop 1
    end if
    checks = checks + 1
    write(*, '(a)') 'PASS: ' // description
  end subroutine

  subroutine wait_milliseconds(milliseconds)
    integer(c_int64_t), intent(in) :: milliseconds
    integer(c_int64_t) :: started, current, rate
    call system_clock(started, rate)
    if (rate <= 0_c_int64_t) error stop 'system_clock has no monotonic counter'
    do
      call system_clock(current)
      if (current - started >= rate * milliseconds / 1000_c_int64_t) exit
    end do
  end subroutine

  subroutine run_redis()
    provider_a = c_null_ptr
    provider_b = c_null_ptr
    provider_c = c_null_ptr
    lock_a = c_null_ptr
    lock_b = c_null_ptr
    lock_c = c_null_ptr
    lease_a = c_null_ptr
    lease_b = c_null_ptr
    lease_c = c_null_ptr

    status = redis_open(trim(epoch), provider_a, message, host=trim(endpoint), &
                        port=port, name_space=trim(namespace))
    call require(status == dilock_ok, 'Redis provider A connects to provisioned namespace: ' // trim(message))
    status = redis_open(trim(epoch), provider_b, message, host=trim(endpoint), &
                        port=port, name_space=trim(namespace))
    call require(status == dilock_ok, 'Redis provider B uses an independent C ABI transport: ' // trim(message))
    status = redis_open(trim(epoch), provider_c, message, host=trim(endpoint), &
                        port=port, name_space=trim(namespace))
    call require(status == dilock_ok, 'Redis provider C uses an independent C ABI transport: ' // trim(message))
    status = redis_lock(provider_a, 'fortran/live/resource', lock_a, message)
    call require(status == dilock_ok, 'Redis resource A created: ' // trim(message))
    status = redis_lock(provider_b, 'fortran/live/resource', lock_b, message)
    call require(status == dilock_ok, 'Redis resource B created: ' // trim(message))
    status = redis_lock(provider_c, 'fortran/live/resource', lock_c, message)
    call require(status == dilock_ok, 'Redis resource C created: ' // trim(message))

    status = redis_acquire(lock_a, 5000_c_int64_t, 2500_c_int64_t, lease_a, message)
    call require(status == dilock_ok, 'Redis owner acquires a real lease: ' // trim(message))
    status = redis_try_acquire(lock_b, 2500_c_int64_t, lease_b, message)
    call require(status == dilock_busy .and. .not. c_associated(lease_b), &
                  'Redis independent contender receives BUSY while owner is live: ' // trim(message))

    call wait_milliseconds(3000_c_int64_t)
    status = redis_try_acquire(lock_b, 2500_c_int64_t, lease_b, message)
    call require(status == dilock_ok, 'Redis successor acquires after server TTL expiry: ' // trim(message))
    status = redis_release(lease_a, outcome, message)
    call require(status == dilock_ok .and. outcome == dilock_already_not_owned, &
                  'Redis delayed old-owner release is acknowledged as already-not-owned: ' // trim(message))
    status = redis_try_acquire(lock_c, 2500_c_int64_t, lease_c, message)
    call require(status == dilock_busy .and. .not. c_associated(lease_c), &
                  'Redis delayed stale release preserves the successor key: ' // trim(message))
    status = redis_release(lease_b, outcome, message)
    call require(status == dilock_ok .and. outcome == dilock_released, &
                  'Redis successor release is acknowledged: ' // trim(message))
    status = redis_try_acquire(lock_c, 2500_c_int64_t, lease_c, message)
    call require(status == dilock_ok, 'Redis handoff permits a new owner: ' // trim(message))
    status = redis_release(lease_c, outcome, message)
    call require(status == dilock_ok .and. outcome == dilock_released, 'Redis final owner releases cleanly: ' // trim(message))

    call redis_destroy_lease(lease_c)
    call redis_destroy_lease(lease_b)
    call redis_destroy_lease(lease_a)
    call redis_destroy_lock(lock_c)
    call redis_destroy_lock(lock_b)
    call redis_destroy_lock(lock_a)
    call redis_destroy_provider(provider_c)
    call redis_destroy_provider(provider_b)
    call redis_destroy_provider(provider_a)
  end subroutine

  subroutine run_etcd()
    provider_a = c_null_ptr
    provider_b = c_null_ptr
    provider_c = c_null_ptr
    lock_a = c_null_ptr
    lock_b = c_null_ptr
    lock_c = c_null_ptr
    lease_a = c_null_ptr
    lease_b = c_null_ptr
    lease_c = c_null_ptr

    status = etcd_open(trim(endpoint), trim(epoch), provider_a, message, &
                       name_space=trim(namespace), auto_renew=.false.)
    call require(status == dilock_ok, 'etcd provider A connects to provisioned namespace: ' // trim(message))
    status = etcd_open(trim(endpoint), trim(epoch), provider_b, message, &
                       name_space=trim(namespace), auto_renew=.false.)
    call require(status == dilock_ok, 'etcd provider B uses an independent C ABI transport: ' // trim(message))
    status = etcd_open(trim(endpoint), trim(epoch), provider_c, message, &
                       name_space=trim(namespace), auto_renew=.false.)
    call require(status == dilock_ok, 'etcd provider C uses an independent C ABI transport: ' // trim(message))
    status = etcd_lock(provider_a, 'fortran/live/resource', lock_a, message)
    call require(status == dilock_ok, 'etcd resource A created: ' // trim(message))
    status = etcd_lock(provider_b, 'fortran/live/resource', lock_b, message)
    call require(status == dilock_ok, 'etcd resource B created: ' // trim(message))
    status = etcd_lock(provider_c, 'fortran/live/resource', lock_c, message)
    call require(status == dilock_ok, 'etcd resource C created: ' // trim(message))

    status = etcd_try_acquire(lock_a, 3000_c_int64_t, lease_a, message)
    call require(status == dilock_unsupported .and. .not. c_associated(lease_a), &
                  'etcd try-acquire reports unsupported without returning a lease: ' // trim(message))

    status = etcd_acquire(lock_a, 10000_c_int64_t, 3000_c_int64_t, lease_a, message)
    call require(status == dilock_ok, 'etcd native Lock acquires a real lease: ' // trim(message))
    fence_epoch = ''
    status = etcd_fence(lease_a, revision_a, fence_epoch, message)
    call require(status == dilock_ok .and. revision_a > 0_c_int64_t, &
                  'etcd lease returns a positive create_revision fence: ' // trim(message))
    call require(trim(fence_epoch) == trim(epoch), 'etcd fencing epoch matches configured epoch')

    status = etcd_acquire(lock_b, 300_c_int64_t, 3000_c_int64_t, lease_b, message)
    call require(status == dilock_wait_timeout .and. .not. c_associated(lease_b), &
                  'etcd queued contender returns bounded wait-timeout: ' // trim(message))
    call wait_milliseconds(3600_c_int64_t)
    status = etcd_acquire(lock_b, 10000_c_int64_t, 5000_c_int64_t, lease_b, message)
    call require(status == dilock_ok, 'etcd successor acquires after owner lease expiry: ' // trim(message))
    block
      character(len=512) :: successor_epoch
      successor_epoch = ''
      status = etcd_fence(lease_b, revision_b, successor_epoch, message)
      call require(status == dilock_ok .and. revision_b > revision_a, &
                    'etcd successor fencing revision increases: ' // trim(message))
      call require(trim(successor_epoch) == trim(epoch), 'etcd fencing epoch matches configured epoch')
    end block
    status = etcd_release(lease_a, outcome, message)
    write(*, '(a,i0)') 'ETCD_STALE_OWNER_RELEASE_OUTCOME: ', outcome
    call require(status == dilock_ok .and. outcome >= dilock_released .and. &
                  outcome <= dilock_release_unknown, &
                  'etcd delayed old-owner unlock returns a defined ABI outcome: ' // trim(message))
    status = etcd_acquire(lock_c, 300_c_int64_t, 3000_c_int64_t, lease_c, message)
    call require(status == dilock_wait_timeout .and. .not. c_associated(lease_c), &
                  'etcd delayed stale unlock leaves successor locked: ' // trim(message))
    status = etcd_release(lease_b, outcome, message)
    call require(status == dilock_ok .and. outcome == dilock_released, 'etcd successor release is acknowledged: ' // trim(message))
    status = etcd_acquire(lock_c, 10000_c_int64_t, 5000_c_int64_t, lease_c, message)
    call require(status == dilock_ok, 'etcd handoff permits a new owner: ' // trim(message))
    status = etcd_release(lease_c, outcome, message)
    call require(status == dilock_ok .and. outcome == dilock_released, 'etcd final owner releases cleanly: ' // trim(message))

    call etcd_destroy_lease(lease_c)
    call etcd_destroy_lease(lease_b)
    call etcd_destroy_lease(lease_a)
    call etcd_destroy_lock(lock_c)
    call etcd_destroy_lock(lock_b)
    call etcd_destroy_lock(lock_a)
    call etcd_destroy_provider(provider_c)
    call etcd_destroy_provider(provider_b)
    call etcd_destroy_provider(provider_a)
  end subroutine
end program live_backend_consumer
