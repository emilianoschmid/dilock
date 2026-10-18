cmake_minimum_required(VERSION 3.24)
project(DilockCppArtifactConsumer LANGUAGES CXX)

find_package(DilockCpp 0.1 CONFIG REQUIRED)
add_executable(dilock_cpp_artifact_consumer main.cpp)
target_compile_features(dilock_cpp_artifact_consumer PRIVATE cxx_std_20)
target_link_libraries(dilock_cpp_artifact_consumer PRIVATE Dilock::redis Dilock::etcd Dilock::postgres)
