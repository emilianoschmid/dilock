cmake_minimum_required(VERSION 3.24)
project(DilockCpp VERSION 0.1.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

option(DILOCK_BUILD_TESTS "Build SDK tests" ON)
option(DILOCK_BUILD_ETCD "Build the optional etcd native Lock profile" OFF)
option(DILOCK_BUILD_C_API "Build the optional C17 ABI facade over both native backend profiles" OFF)
option(DILOCK_ENABLE_ASAN_UBSAN "Instrument SDK and tests with AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(DILOCK_ENABLE_TSAN "Instrument SDK and tests with ThreadSanitizer" OFF)

if(DILOCK_BUILD_C_API)
    enable_language(C)
endif()

if(DILOCK_ENABLE_ASAN_UBSAN AND DILOCK_ENABLE_TSAN)
    message(FATAL_ERROR "ASan/UBSan and TSan require separate builds")
endif()

find_package(OpenSSL 3.0 REQUIRED COMPONENTS Crypto)
find_package(nlohmann_json 3.11 CONFIG REQUIRED)
find_package(redis++ 1.3.15 CONFIG REQUIRED)
find_package(Threads REQUIRED)

if(TARGET redis++_static)
    set(DILOCK_REDISPP_TARGET redis++_static)
elseif(TARGET redis++::redis++_static)
    set(DILOCK_REDISPP_TARGET redis++::redis++_static)
elseif(TARGET redis++_shared)
    set(DILOCK_REDISPP_TARGET redis++_shared)
elseif(TARGET redis++::redis++)
    set(DILOCK_REDISPP_TARGET redis++::redis++)
elseif(TARGET redis++)
    set(DILOCK_REDISPP_TARGET redis++)
else()
    message(FATAL_ERROR "redis-plus-plus was found, but exported no recognized CMake target")
endif()

file(READ "${PROJECT_SOURCE_DIR}/resources/redis-release.lua" DILOCK_RELEASE_SCRIPT)
file(READ "${PROJECT_SOURCE_DIR}/resources/redis-renew.lua" DILOCK_RENEW_SCRIPT)
file(MAKE_DIRECTORY "${PROJECT_BINARY_DIR}/generated")
configure_file(src/redis_scripts.hpp.in generated/redis_scripts.hpp @ONLY)

add_library(dilock_cpp src/redis.cpp)
add_library(Dilock::redis ALIAS dilock_cpp)
target_compile_features(dilock_cpp PUBLIC cxx_std_20)
target_include_directories(dilock_cpp
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE "${PROJECT_BINARY_DIR}/generated")
target_link_libraries(dilock_cpp
    PUBLIC OpenSSL::Crypto
    PRIVATE ${DILOCK_REDISPP_TARGET} nlohmann_json::nlohmann_json)
add_executable(dilock_cpp_worker worker.cpp)
target_compile_features(dilock_cpp_worker PRIVATE cxx_std_20)
target_link_libraries(dilock_cpp_worker PRIVATE Dilock::redis nlohmann_json::nlohmann_json Threads::Threads)
set_target_properties(dilock_cpp PROPERTIES
    EXPORT_NAME redis
    CXX_EXTENSIONS OFF
    POSITION_INDEPENDENT_CODE ON)

if(DILOCK_ENABLE_ASAN_UBSAN OR DILOCK_ENABLE_TSAN)
    if(MSVC)
        message(FATAL_ERROR "The selected sanitizer profile is not supported by this CMake configuration on MSVC")
    endif()
    if(DILOCK_ENABLE_TSAN)
        set(DILOCK_SANITIZER_FLAGS -fsanitize=thread -fno-omit-frame-pointer)
    else()
        set(DILOCK_SANITIZER_FLAGS -fsanitize=address,undefined -fno-omit-frame-pointer)
    endif()
    target_compile_options(dilock_cpp PRIVATE ${DILOCK_SANITIZER_FLAGS})
    target_link_options(dilock_cpp PRIVATE ${DILOCK_SANITIZER_FLAGS})
    target_compile_options(dilock_cpp_worker PRIVATE ${DILOCK_SANITIZER_FLAGS})
    target_link_options(dilock_cpp_worker PRIVATE ${DILOCK_SANITIZER_FLAGS})
endif()

if(MSVC)
    target_compile_options(dilock_cpp PRIVATE /W4 /permissive-)
    target_compile_options(dilock_cpp_worker PRIVATE /W4 /permissive-)
else()
    target_compile_options(dilock_cpp PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion)
    target_compile_options(dilock_cpp_worker PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion)
endif()

if(DILOCK_BUILD_TESTS)
    enable_testing()
    add_executable(dilock_c_api_error_test ../c/tests/c_api_error_test.cpp)
    target_compile_features(dilock_c_api_error_test PRIVATE cxx_std_20)
    target_include_directories(dilock_c_api_error_test PRIVATE "${PROJECT_SOURCE_DIR}/../c/include")
    add_test(NAME dilock_c_api_error COMMAND dilock_c_api_error_test)

    add_library(dilock_cpp_api_defaults OBJECT tests/api_defaults_compile.cpp)
    target_compile_features(dilock_cpp_api_defaults PRIVATE cxx_std_20)
    target_include_directories(dilock_cpp_api_defaults PRIVATE "${PROJECT_SOURCE_DIR}/include")
    if(MSVC)
        target_compile_options(dilock_cpp_api_defaults PRIVATE /W4 /permissive-)
    else()
        target_compile_options(dilock_cpp_api_defaults PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion)
    endif()

    add_executable(dilock_cpp_identity_test tests/identity_test.cpp)
    target_compile_features(dilock_cpp_identity_test PRIVATE cxx_std_20)
    target_include_directories(dilock_cpp_identity_test PRIVATE
        "${PROJECT_BINARY_DIR}/generated"
        "${PROJECT_SOURCE_DIR}/src")
    target_link_libraries(dilock_cpp_identity_test PRIVATE Dilock::redis nlohmann_json::nlohmann_json)
    target_compile_definitions(dilock_cpp_identity_test PRIVATE
        DILOCK_IDENTITY_VECTORS="${PROJECT_SOURCE_DIR}/../../spec/vectors/identity.json"
        DILOCK_RELEASE_SOURCE="${PROJECT_SOURCE_DIR}/../../spec/backends/redis-release.lua"
        DILOCK_RENEW_SOURCE="${PROJECT_SOURCE_DIR}/../../spec/backends/redis-renew.lua")
    add_test(NAME dilock_cpp_identity COMMAND dilock_cpp_identity_test)

    add_executable(dilock_cpp_redis_wait_validation_test tests/redis_wait_validation_test.cpp)
    target_compile_features(dilock_cpp_redis_wait_validation_test PRIVATE cxx_std_20)
    target_link_libraries(dilock_cpp_redis_wait_validation_test PRIVATE Dilock::redis)
    add_test(NAME dilock_cpp_redis_wait_validation COMMAND dilock_cpp_redis_wait_validation_test)
    if(MSVC)
        target_compile_options(dilock_cpp_redis_wait_validation_test PRIVATE /W4 /permissive-)
    else()
        target_compile_options(dilock_cpp_redis_wait_validation_test PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion)
    endif()

    add_executable(dilock_cpp_redis_integration tests/redis_integration.cpp)
    target_compile_features(dilock_cpp_redis_integration PRIVATE cxx_std_20)
    target_link_libraries(dilock_cpp_redis_integration PRIVATE Dilock::redis ${DILOCK_REDISPP_TARGET})
    add_test(NAME dilock_cpp_redis_integration COMMAND dilock_cpp_redis_integration)
    set_tests_properties(dilock_cpp_redis_integration PROPERTIES SKIP_RETURN_CODE 77)

    if(DILOCK_ENABLE_ASAN_UBSAN OR DILOCK_ENABLE_TSAN)
        foreach(test_target dilock_cpp_identity_test dilock_cpp_redis_wait_validation_test dilock_cpp_redis_integration)
            target_compile_options(${test_target} PRIVATE ${DILOCK_SANITIZER_FLAGS})
            target_link_options(${test_target} PRIVATE ${DILOCK_SANITIZER_FLAGS})
        endforeach()
    endif()
endif()

if(DILOCK_BUILD_ETCD)
    find_package(OpenSSL 3.0 REQUIRED COMPONENTS Crypto SSL)
    find_package(Protobuf 3.21 REQUIRED)
    find_package(gRPC 1.51 CONFIG REQUIRED)

    set(DILOCK_ETCD_PROTO_DIR "${PROJECT_SOURCE_DIR}/proto/etcd-v3.7.2")
    set(DILOCK_ETCD_GENERATED_DIR "${PROJECT_BINARY_DIR}/generated/etcd")
    file(MAKE_DIRECTORY "${DILOCK_ETCD_GENERATED_DIR}")
    set(DILOCK_ETCD_PROTO_NAMES kv rpc v3lock)
    set(DILOCK_ETCD_GENERATED_SOURCES)
    set(DILOCK_ETCD_GENERATED_HEADERS)
    foreach(proto IN LISTS DILOCK_ETCD_PROTO_NAMES)
        list(APPEND DILOCK_ETCD_GENERATED_SOURCES
            "${DILOCK_ETCD_GENERATED_DIR}/${proto}.pb.cc"
            "${DILOCK_ETCD_GENERATED_DIR}/${proto}.grpc.pb.cc")
        list(APPEND DILOCK_ETCD_GENERATED_HEADERS
            "${DILOCK_ETCD_GENERATED_DIR}/${proto}.pb.h"
            "${DILOCK_ETCD_GENERATED_DIR}/${proto}.grpc.pb.h")
        add_custom_command(
            OUTPUT
                "${DILOCK_ETCD_GENERATED_DIR}/${proto}.pb.cc"
                "${DILOCK_ETCD_GENERATED_DIR}/${proto}.pb.h"
                "${DILOCK_ETCD_GENERATED_DIR}/${proto}.grpc.pb.cc"
                "${DILOCK_ETCD_GENERATED_DIR}/${proto}.grpc.pb.h"
            COMMAND $<TARGET_FILE:protobuf::protoc>
                "--proto_path=${DILOCK_ETCD_PROTO_DIR}"
                "--cpp_out=${DILOCK_ETCD_GENERATED_DIR}"
                "--grpc_out=${DILOCK_ETCD_GENERATED_DIR}"
                "--plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
                "${DILOCK_ETCD_PROTO_DIR}/${proto}.proto"
            DEPENDS
                "${DILOCK_ETCD_PROTO_DIR}/${proto}.proto"
                "${DILOCK_ETCD_PROTO_DIR}/rpc.proto"
                "${DILOCK_ETCD_PROTO_DIR}/kv.proto"
                "${DILOCK_ETCD_PROTO_DIR}/v3lock.proto"
                protobuf::protoc gRPC::grpc_cpp_plugin
            VERBATIM)
    endforeach()
    add_library(dilock_cpp_etcd src/etcd.cpp ${DILOCK_ETCD_GENERATED_SOURCES} ${DILOCK_ETCD_GENERATED_HEADERS})
    add_library(Dilock::etcd ALIAS dilock_cpp_etcd)
    target_compile_features(dilock_cpp_etcd PUBLIC cxx_std_20)
    target_include_directories(dilock_cpp_etcd
        PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
               $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE "${DILOCK_ETCD_GENERATED_DIR}")
    target_link_libraries(dilock_cpp_etcd
        PUBLIC gRPC::grpc++ OpenSSL::SSL
        PRIVATE OpenSSL::Crypto nlohmann_json::nlohmann_json protobuf::libprotobuf)
    target_link_libraries(dilock_cpp_worker PRIVATE Dilock::etcd)
    target_compile_definitions(dilock_cpp_worker PRIVATE DILOCK_WORKER_ETCD=1)
    set_target_properties(dilock_cpp_etcd PROPERTIES
        EXPORT_NAME etcd
        CXX_EXTENSIONS OFF
        POSITION_INDEPENDENT_CODE ON)
    if(MSVC)
        target_compile_options(dilock_cpp_etcd PRIVATE /W4 /permissive-)
    else()
        target_compile_options(dilock_cpp_etcd PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion)
    endif()
    if(DILOCK_BUILD_TESTS)
        add_library(dilock_cpp_etcd_api_defaults OBJECT tests/etcd_api_defaults_compile.cpp)
        target_compile_features(dilock_cpp_etcd_api_defaults PRIVATE cxx_std_20)
        target_link_libraries(dilock_cpp_etcd_api_defaults PRIVATE Dilock::etcd)
        if(MSVC)
            target_compile_options(dilock_cpp_etcd_api_defaults PRIVATE /W4 /permissive-)
        else()
            target_compile_options(dilock_cpp_etcd_api_defaults PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion)
        endif()

        add_executable(dilock_cpp_etcd_deadline_test tests/etcd_deadline_test.cpp)
        target_compile_features(dilock_cpp_etcd_deadline_test PRIVATE cxx_std_20)
        target_link_libraries(dilock_cpp_etcd_deadline_test PRIVATE Dilock::etcd Threads::Threads)
        add_test(NAME dilock_cpp_etcd_deadline COMMAND dilock_cpp_etcd_deadline_test)

        add_executable(dilock_cpp_etcd_integration tests/etcd_integration.cpp)
        target_compile_features(dilock_cpp_etcd_integration PRIVATE cxx_std_20)
        target_include_directories(dilock_cpp_etcd_integration PRIVATE "${DILOCK_ETCD_GENERATED_DIR}")
        target_link_libraries(dilock_cpp_etcd_integration PRIVATE Dilock::etcd nlohmann_json::nlohmann_json)
        add_test(NAME dilock_cpp_etcd_integration COMMAND dilock_cpp_etcd_integration)
        set_tests_properties(dilock_cpp_etcd_integration PROPERTIES SKIP_RETURN_CODE 77)
        if(DILOCK_ENABLE_ASAN_UBSAN OR DILOCK_ENABLE_TSAN)
            foreach(test_target dilock_cpp_etcd_deadline_test dilock_cpp_etcd_integration)
                target_compile_options(${test_target} PRIVATE ${DILOCK_SANITIZER_FLAGS})
                target_link_options(${test_target} PRIVATE ${DILOCK_SANITIZER_FLAGS})
            endforeach()
        endif()
    endif()
    install(TARGETS dilock_cpp_etcd EXPORT DilockCppTargets
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")
    install(FILES proto/etcd-v3.7.2/LICENSE-etcd-api proto/etcd-v3.7.2/LICENSE-etcd-server proto/etcd-v3.7.2/UPSTREAM.md
        DESTINATION "${CMAKE_INSTALL_DATADIR}/licenses/DilockCpp/etcd")
endif()

if(DILOCK_BUILD_C_API)
    if(NOT DILOCK_BUILD_ETCD)
        message(FATAL_ERROR "DILOCK_BUILD_C_API requires DILOCK_BUILD_ETCD=ON so the C ABI exposes the configured Redis and etcd profiles together")
    endif()
    add_library(dilock_c ../c/src/dilock_c.cpp)
    add_library(Dilock::c ALIAS dilock_c)
    target_compile_features(dilock_c PUBLIC cxx_std_20)
    target_include_directories(dilock_c PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/../c/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
    target_link_libraries(dilock_c PUBLIC Dilock::redis Dilock::etcd PRIVATE Threads::Threads)
    # Static C ABI consumers may link with a C or Fortran driver, which does
    # not add the C++ standard library implicitly. Propagate the first
    # toolchain-provided C++ runtime library with the exported target.
    if(CMAKE_CXX_IMPLICIT_LINK_LIBRARIES)
        list(GET CMAKE_CXX_IMPLICIT_LINK_LIBRARIES 0 DILOCK_CXX_RUNTIME_LIBRARY)
        target_link_libraries(dilock_c PUBLIC "${DILOCK_CXX_RUNTIME_LIBRARY}")
    endif()
    if(BUILD_TESTING AND DILOCK_CXX_RUNTIME_LIBRARY)
        get_target_property(DILOCK_C_LINK_INTERFACE Dilock::c INTERFACE_LINK_LIBRARIES)
        if(NOT DILOCK_CXX_RUNTIME_LIBRARY IN_LIST DILOCK_C_LINK_INTERFACE)
            message(FATAL_ERROR "Dilock::c must propagate the C++ runtime for non-C++ static-library consumers")
        endif()
    endif()
    set_target_properties(dilock_c PROPERTIES EXPORT_NAME c CXX_EXTENSIONS OFF)
    if(BUILD_SHARED_LIBS)
        target_compile_definitions(dilock_c PUBLIC DILOCK_C_SHARED PRIVATE DILOCK_C_BUILD)
    endif()
    if(MSVC)
        target_compile_options(dilock_c PRIVATE /W4 /permissive-)
    else()
        target_compile_options(dilock_c PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion)
    endif()

    foreach(backend IN ITEMS redis etcd)
        add_executable(dilock_c_${backend}_quickstart ../c/examples/${backend}_quickstart.c)
        target_link_libraries(dilock_c_${backend}_quickstart PRIVATE Dilock::c)
        set_target_properties(dilock_c_${backend}_quickstart PROPERTIES
            C_STANDARD 17
            C_STANDARD_REQUIRED ON
            C_EXTENSIONS OFF)
        if(MSVC)
            target_compile_options(dilock_c_${backend}_quickstart PRIVATE /W4 /WX)
        else()
            target_compile_options(dilock_c_${backend}_quickstart PRIVATE -Wall -Wextra -Wpedantic -Werror)
        endif()
    endforeach()

    if(DILOCK_BUILD_TESTS)
        add_executable(dilock_c_api_consumer ../c/tests/c_api_consumer.c)
        target_link_libraries(dilock_c_api_consumer PRIVATE Dilock::c)
        set_target_properties(dilock_c_api_consumer PROPERTIES C_STANDARD 17 C_STANDARD_REQUIRED ON C_EXTENSIONS OFF)
        if(MSVC)
            target_compile_options(dilock_c_api_consumer PRIVATE /W4 /WX)
        else()
            target_compile_options(dilock_c_api_consumer PRIVATE -Wall -Wextra -Wpedantic -Werror)
        endif()
        add_test(NAME dilock_c_api_consumer COMMAND dilock_c_api_consumer)
    endif()
endif()

install(TARGETS dilock_cpp EXPORT DilockCppTargets
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")
install(FILES LICENSE DESTINATION "${CMAKE_INSTALL_DATADIR}/licenses/DilockCpp")
install(DIRECTORY include/ DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
if(DILOCK_BUILD_C_API)
    install(TARGETS dilock_c EXPORT DilockCppTargets
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")
    install(FILES "${PROJECT_SOURCE_DIR}/../c/include/dilock/dilock.h"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/dilock")
endif()
install(EXPORT DilockCppTargets
    FILE DilockCppTargets.cmake
    NAMESPACE Dilock::
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/DilockCpp")
configure_package_config_file(cmake/DilockCppConfig.cmake.in
    "${PROJECT_BINARY_DIR}/DilockCppConfig.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/DilockCpp")
write_basic_package_version_file("${PROJECT_BINARY_DIR}/DilockCppConfigVersion.cmake"
    VERSION "${PROJECT_VERSION}" COMPATIBILITY SameMajorVersion)
install(FILES
    "${PROJECT_BINARY_DIR}/DilockCppConfig.cmake"
    "${PROJECT_BINARY_DIR}/DilockCppConfigVersion.cmake"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/DilockCpp")
