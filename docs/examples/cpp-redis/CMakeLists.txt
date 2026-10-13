cmake_minimum_required(VERSION 3.24)
project(DilockCppRedisConsumer LANGUAGES CXX)

find_package(DilockCpp 0.1 CONFIG REQUIRED)

add_executable(dilock_cpp_redis_consumer main.cpp)
target_compile_features(dilock_cpp_redis_consumer PRIVATE cxx_std_20)
target_link_libraries(dilock_cpp_redis_consumer PRIVATE Dilock::redis)

if(MSVC)
    target_compile_options(dilock_cpp_redis_consumer PRIVATE /W4 /permissive-)
else()
    target_compile_options(dilock_cpp_redis_consumer PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion)
endif()
