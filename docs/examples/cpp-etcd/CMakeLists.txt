cmake_minimum_required(VERSION 3.24)
project(DilockCppEtcdConsumer LANGUAGES CXX)

find_package(DilockCpp 0.1 CONFIG REQUIRED)
add_executable(dilock_cpp_etcd_consumer main.cpp)
target_compile_features(dilock_cpp_etcd_consumer PRIVATE cxx_std_20)
target_link_libraries(dilock_cpp_etcd_consumer PRIVATE Dilock::etcd)
