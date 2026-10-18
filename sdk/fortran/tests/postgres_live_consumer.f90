program postgres_live_consumer
  use, intrinsic :: iso_c_binding, only: c_ptr, c_null_ptr, c_int, c_associated
  use dilock_fortran
  implicit none

  character(len=512) :: connection_string, namespace, epoch, message
  type(c_ptr) :: provider_a = c_null_ptr, provider_b = c_null_ptr
  type(c_ptr) :: lock_a = c_null_ptr, lock_b = c_null_ptr
  type(c_ptr) :: lease_a = c_null_ptr, lease_b = c_null_ptr
  integer(c_int) :: status, outcome, renewed

  call get_environment_variable('DILOCK_POSTGRES_DSN', connection_string)
  if (len_trim(connection_string) == 0) error stop 'set DILOCK_POSTGRES_DSN'
  namespace = 'fortran-postgres-live'
  epoch = 'fortran-postgres-live-epoch'

  status = postgres_provision_namespace(trim(connection_string), trim(epoch), message, &
                                        name_space=trim(namespace))
  call require(status == dilock_ok, 'PostgreSQL metadata provisioning: ' // trim(message))
  status = postgres_open(trim(connection_string), trim(epoch), provider_a, message, &
                         name_space=trim(namespace))
  call require(status == dilock_ok, 'PostgreSQL provider A opens: ' // trim(message))
  status = postgres_open(trim(connection_string), trim(epoch), provider_b, message, &
                         name_space=trim(namespace))
  call require(status == dilock_ok, 'PostgreSQL provider B opens independently: ' // trim(message))
  status = postgres_lock(provider_a, 'fortran/postgres-live', lock_a, message)
  call require(status == dilock_ok, 'PostgreSQL resource A is created: ' // trim(message))
  status = postgres_lock(provider_b, 'fortran/postgres-live', lock_b, message)
  call require(status == dilock_ok, 'PostgreSQL resource B is created: ' // trim(message))

  status = postgres_try_acquire(lock_a, lease_a, message)
  call require(status == dilock_ok .and. c_associated(lease_a), &
               'PostgreSQL owner acquires through the Fortran facade: ' // trim(message))
  call require(postgres_is_valid(lease_a) /= 0, 'PostgreSQL owner lease is valid')
  status = postgres_try_acquire(lock_b, lease_b, message)
  call require(status == dilock_busy .and. .not. c_associated(lease_b), &
               'PostgreSQL independent contender receives BUSY: ' // trim(message))
  status = postgres_renew(lease_a, renewed, message)
  call require(status == dilock_ok .and. renewed /= 0, 'PostgreSQL liveness check succeeds: ' // trim(message))
  status = postgres_release(lease_a, outcome, message)
  call require(status == dilock_ok .and. outcome == dilock_released, &
               'PostgreSQL owner release is acknowledged: ' // trim(message))
  call postgres_destroy_lease(lease_a)

  status = postgres_try_acquire(lock_b, lease_b, message)
  call require(status == dilock_ok .and. c_associated(lease_b), &
               'PostgreSQL successor acquires after release: ' // trim(message))
  status = postgres_release(lease_b, outcome, message)
  call require(status == dilock_ok .and. outcome == dilock_released, &
               'PostgreSQL successor release is acknowledged: ' // trim(message))

  call postgres_destroy_lease(lease_b)
  call postgres_destroy_lock(lock_b)
  call postgres_destroy_lock(lock_a)
  call postgres_destroy_provider(provider_b)
  call postgres_destroy_provider(provider_a)
  write(*, '(a)') 'PASS Fortran PostgreSQL: provision, contention, liveness, release, and handoff'

contains
  subroutine require(condition, description)
    logical, intent(in) :: condition
    character(len=*), intent(in) :: description
    if (.not. condition) error stop 'FAIL: ' // description
  end subroutine require
end program postgres_live_consumer
