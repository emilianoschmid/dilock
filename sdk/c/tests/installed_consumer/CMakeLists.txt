cmake_minimum_required(VERSION 3.24)
project(DilockInstalledC17Consumer LANGUAGES C CXX)

find_package(DilockCpp 0.1 CONFIG REQUIRED)

add_executable(dilock_installed_c17_consumer main.c)
set_target_properties(dilock_installed_c17_consumer PROPERTIES
    C_STANDARD 17
    C_STANDARD_REQUIRED ON
    C_EXTENSIONS OFF
    LINKER_LANGUAGE CXX)
target_compile_options(dilock_installed_c17_consumer PRIVATE
    -Wall -Wextra -Wpedantic -Werror)
target_link_libraries(dilock_installed_c17_consumer PRIVATE Dilock::c)

add_executable(dilock_installed_c17_etcd_consumer etcd_main.c)
set_target_properties(dilock_installed_c17_etcd_consumer PROPERTIES
    C_STANDARD 17
    C_STANDARD_REQUIRED ON
    C_EXTENSIONS OFF
    LINKER_LANGUAGE CXX)
target_compile_options(dilock_installed_c17_etcd_consumer PRIVATE
    -Wall -Wextra -Wpedantic -Werror)
target_link_libraries(dilock_installed_c17_etcd_consumer PRIVATE Dilock::c)
