cmake_minimum_required(VERSION 3.24)
project(DilockFortran VERSION 0.1.0 LANGUAGES C Fortran)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CTest)

find_package(DilockCpp CONFIG REQUIRED)
if(NOT TARGET Dilock::c)
  message(FATAL_ERROR "Install DilockCpp with DILOCK_BUILD_C_API=ON and both backend profiles")
endif()

add_library(DilockFortran dilock_fortran.f90)
target_link_libraries(DilockFortran PUBLIC Dilock::c)
set_target_properties(DilockFortran PROPERTIES
  EXPORT_NAME fortran
  Fortran_MODULE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/modules"
  Fortran_STANDARD 2008
  Fortran_STANDARD_REQUIRED YES)
target_include_directories(DilockFortran PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/modules>
  $<INSTALL_INTERFACE:include/dilock/fortran>)
if(CMAKE_Fortran_COMPILER_ID STREQUAL "GNU")
  target_compile_options(DilockFortran PRIVATE -Wall -Wextra -Wimplicit-interface -fcheck=all -ffree-line-length-none)
endif()

add_executable(dilock_fortran_redis_example examples/redis_quickstart.f90)
target_link_libraries(dilock_fortran_redis_example PRIVATE DilockFortran)
add_executable(dilock_fortran_etcd_example examples/etcd_quickstart.f90)
target_link_libraries(dilock_fortran_etcd_example PRIVATE DilockFortran)
add_executable(dilock_fortran_etcd_configured_example examples/etcd_configured_quickstart.f90)
target_link_libraries(dilock_fortran_etcd_configured_example PRIVATE DilockFortran)
if(BUILD_TESTING)
  enable_language(C)
  # Building this executable is the compile/link consumer smoke. Running it
  # requires a separately provisioned Redis namespace and endpoint.
  add_executable(dilock_fortran_facade_tests
    dilock_fortran.f90
    tests/facade_test.f90
    tests/c_abi_shim.c)
  set_target_properties(dilock_fortran_facade_tests PROPERTIES
    Fortran_MODULE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/test-modules"
    Fortran_STANDARD 2008
    Fortran_STANDARD_REQUIRED YES)
  target_include_directories(dilock_fortran_facade_tests PRIVATE
    "${CMAKE_CURRENT_BINARY_DIR}/test-modules"
    "${CMAKE_CURRENT_SOURCE_DIR}/../c/include")
  if(CMAKE_Fortran_COMPILER_ID STREQUAL "GNU")
    target_compile_options(dilock_fortran_facade_tests PRIVATE
      $<$<COMPILE_LANGUAGE:Fortran>:-Wall;-Wextra;-Wimplicit-interface;-fcheck=all;-ffree-line-length-none>)
  endif()
  add_test(NAME dilock_fortran_facade_tests COMMAND dilock_fortran_facade_tests)

  # Compile a real-package consumer in every test build. It is run explicitly
  # against a disposable Redis or etcd endpoint; normal CTest remains offline.
  add_executable(dilock_fortran_live_consumer tests/live_backend_consumer.f90)
  target_link_libraries(dilock_fortran_live_consumer PRIVATE DilockFortran)
  set_target_properties(dilock_fortran_live_consumer PROPERTIES
    Fortran_STANDARD 2008
    Fortran_STANDARD_REQUIRED YES)
  if(CMAKE_Fortran_COMPILER_ID STREQUAL "GNU")
    target_compile_options(dilock_fortran_live_consumer PRIVATE
      -Wall -Wextra -Wimplicit-interface -fcheck=all -ffree-line-length-none)
  endif()
endif()

install(TARGETS DilockFortran EXPORT DilockFortranTargets
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/modules/dilock_fortran.mod"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/dilock/fortran")
install(EXPORT DilockFortranTargets
  FILE DilockFortranTargets.cmake
  NAMESPACE DilockFortran::
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/DilockFortran")

configure_package_config_file(cmake/DilockFortranConfig.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/DilockFortranConfig.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/DilockFortran")
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/DilockFortranConfigVersion.cmake"
  VERSION "${PROJECT_VERSION}"
  COMPATIBILITY SameMajorVersion)
install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/DilockFortranConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/DilockFortranConfigVersion.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/DilockFortran")
install(FILES "${PROJECT_SOURCE_DIR}/../../LICENSE"
  DESTINATION "${CMAKE_INSTALL_DATADIR}/licenses/DilockFortran")
