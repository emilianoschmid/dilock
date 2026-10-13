cmake_minimum_required(VERSION 3.24)
project(DilockEtcdQuickstart LANGUAGES CXX)

find_package(DilockCpp 0.1 CONFIG REQUIRED)
add_executable(dilock_etcd_quickstart main.cpp)
target_compile_features(dilock_etcd_quickstart PRIVATE cxx_std_20)
target_link_libraries(dilock_etcd_quickstart PRIVATE Dilock::etcd)
