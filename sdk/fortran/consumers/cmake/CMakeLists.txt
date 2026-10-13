cmake_minimum_required(VERSION 3.24)
project(DilockFortranConsumer LANGUAGES C Fortran)

find_package(DilockFortran 0.1 CONFIG REQUIRED)

add_executable(dilock_fortran_installed_consumer main.f90)
target_link_libraries(dilock_fortran_installed_consumer PRIVATE DilockFortran::fortran)
