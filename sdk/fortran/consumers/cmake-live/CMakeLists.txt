cmake_minimum_required(VERSION 3.24)
project(DilockFortranLiveConsumer LANGUAGES C Fortran)

find_package(DilockFortran 0.1 CONFIG REQUIRED)

add_executable(dilock_fortran_live_consumer main.f90)
target_link_libraries(dilock_fortran_live_consumer PRIVATE DilockFortran::fortran)
set_target_properties(dilock_fortran_live_consumer PROPERTIES
  Fortran_STANDARD 2008
  Fortran_STANDARD_REQUIRED YES)
