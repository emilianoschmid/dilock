program installed_consumer
  use, intrinsic :: iso_c_binding, only: c_int, c_ptr, c_null_ptr, c_associated
  use dilock_fortran
  implicit none

  type(c_ptr) :: provider = c_null_ptr
  character(len=384) :: message
  integer(c_int) :: status

  ! Invalid C-string input is rejected locally before any backend connection.
  status = redis_open('invalid' // achar(0) // 'epoch', provider, message)
  if (status /= dilock_invalid_argument) error stop 'embedded NUL was not rejected'
  if (c_associated(provider)) error stop 'invalid provider handle was returned'
  print '(a)', 'installed DilockFortran package API resolved and passed local validation'
end program
