cmake_minimum_required(VERSION 3.24)
project(DilockCTestWorker LANGUAGES CXX)

find_package(DilockCpp 0.1 CONFIG REQUIRED)
find_package(nlohmann_json 3.11 CONFIG REQUIRED)

add_executable(dilock_c_conformance_worker worker.cpp)
target_compile_features(dilock_c_conformance_worker PRIVATE cxx_std_20)
target_compile_options(dilock_c_conformance_worker PRIVATE
    -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion -Werror)
target_link_libraries(dilock_c_conformance_worker PRIVATE
    Dilock::c nlohmann_json::nlohmann_json)

