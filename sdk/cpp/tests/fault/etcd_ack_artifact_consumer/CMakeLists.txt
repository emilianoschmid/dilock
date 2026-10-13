cmake_minimum_required(VERSION 3.24)
project(DilockCppEtcdAckArtifactConsumer LANGUAGES CXX)

find_package(DilockCpp 0.1 CONFIG REQUIRED)
find_package(Threads REQUIRED)

add_executable(dilock_cpp_etcd_ack_consumer ../../../worker.cpp)
target_compile_features(dilock_cpp_etcd_ack_consumer PRIVATE cxx_std_20)
target_compile_definitions(dilock_cpp_etcd_ack_consumer PRIVATE DILOCK_WORKER_ETCD=1)
target_link_libraries(dilock_cpp_etcd_ack_consumer PRIVATE
    Dilock::redis
    Dilock::etcd
    nlohmann_json::nlohmann_json
    Threads::Threads)
