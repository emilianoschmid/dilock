#!/bin/sh
set -eux

if [ -z "${DILOCK_ETCD_ENDPOINT:-}" ]; then
  printf '%s\n' 'DILOCK_ETCD_ENDPOINT must point to an explicitly disposable etcd instance' >&2
  exit 2
fi

grpc_root=/workspace/.tools/grpc-root/usr
cpp_install=/workspace/.tools/cppinstall
cpp_deps=/workspace/.tools/cppdeps/usr
openssl_prefix=/workspace/.tools/openssl/usr
openssl_runtime=/workspace/.tools/openssl-runtime/usr
prefixes="$grpc_root;$cpp_install;$cpp_deps;$openssl_prefix;$openssl_runtime"
export PATH="$grpc_root/bin:$PATH"
export LD_LIBRARY_PATH="$grpc_root/lib/x86_64-linux-gnu:$cpp_install/lib:$cpp_deps/lib/x86_64-linux-gnu:$openssl_runtime/lib/x86_64-linux-gnu:/usr/lib/x86_64-linux-gnu"
export PKG_CONFIG="$grpc_root/bin/pkg-config"
export PKG_CONFIG_SYSROOT_DIR=/workspace/.tools/grpc-root
export PKG_CONFIG_PATH="$grpc_root/lib/x86_64-linux-gnu/pkgconfig"
export PKG_CONFIG_LIBDIR="$grpc_root/lib/x86_64-linux-gnu/pkgconfig"

cmake -S /workspace/sdk/cpp -B /tmp/c-current-build -G 'Unix Makefiles' \
  -DCMAKE_BUILD_TYPE=Release \
  "-DCMAKE_PREFIX_PATH=$prefixes" \
  -DCMAKE_INSTALL_PREFIX=/tmp/c-current-prefix \
  -DBUILD_SHARED_LIBS=ON -DDILOCK_BUILD_TESTS=OFF \
  -DDILOCK_BUILD_ETCD=ON -DDILOCK_BUILD_CONSUL=ON -DDILOCK_BUILD_C_API=ON \
  -DOPENSSL_ROOT_DIR="$openssl_prefix" \
  -DOPENSSL_INCLUDE_DIR="$openssl_prefix/include" \
  -DOPENSSL_CRYPTO_LIBRARY="$openssl_runtime/lib/x86_64-linux-gnu/libcrypto.so.3" \
  -DOPENSSL_SSL_LIBRARY="$openssl_runtime/lib/x86_64-linux-gnu/libssl.so.3" \
  -DOPENSSL_USE_STATIC_LIBS=FALSE \
  -DZLIB_LIBRARY=/usr/lib/x86_64-linux-gnu/libz.so.1 \
  -DZLIB_INCLUDE_DIR="$grpc_root/include" \
  -DgRPC_DIR="$grpc_root/lib/x86_64-linux-gnu/cmake/grpc" \
  -DProtobuf_INCLUDE_DIR="$grpc_root/include" \
  -DProtobuf_LIBRARY="$grpc_root/lib/x86_64-linux-gnu/libprotobuf.so" \
  -DProtobuf_PROTOC_EXECUTABLE="$grpc_root/bin/protoc" \
  -DPKG_CONFIG_EXECUTABLE="$grpc_root/bin/pkg-config" \
  -Dnlohmann_json_DIR="$cpp_deps/share/cmake/nlohmann_json" \
  -Dredis++_DIR="$cpp_install/share/cmake/redis++" \
  -Dhiredis_DIR="$cpp_install/lib/cmake/hiredis"
cmake --build /tmp/c-current-build --parallel 3
cmake --install /tmp/c-current-build

# The source-hidden external-consumer runner uses a first container only to
# build/install Dilock and stage the install plus consumer project in a private
# Docker volume. A separate container then compiles the consumer without the
# checkout mounted at all.
if [ "${DILOCK_INSTALL_ONLY:-0}" = "1" ]; then
  mkdir -p /artifacts
  cp -a /tmp/c-current-prefix /artifacts/prefix
  cp -a /workspace/sdk/c/tests/installed_consumer /artifacts/consumer
  find /artifacts -type f -print0 | sort -z | xargs -0 sha256sum
  exit 0
fi

export CMAKE_PREFIX_PATH="/tmp/c-current-prefix;$prefixes"
cmake -S /workspace/sdk/c/tests/installed_consumer -B /tmp/c17-installed-current \
  -G 'Unix Makefiles' -DCMAKE_BUILD_TYPE=Release \
  -DDilockCpp_DIR=/tmp/c-current-prefix/lib/cmake/DilockCpp \
  "-DCMAKE_PREFIX_PATH=/tmp/c-current-prefix;$prefixes" \
  -DOPENSSL_ROOT_DIR="$openssl_prefix" \
  -DOPENSSL_INCLUDE_DIR="$openssl_prefix/include" \
  -DOPENSSL_CRYPTO_LIBRARY="$openssl_runtime/lib/x86_64-linux-gnu/libcrypto.so.3" \
  -DOPENSSL_SSL_LIBRARY="$openssl_runtime/lib/x86_64-linux-gnu/libssl.so.3" \
  -DOPENSSL_USE_STATIC_LIBS=FALSE \
  -DZLIB_LIBRARY=/usr/lib/x86_64-linux-gnu/libz.so.1 \
  -DZLIB_INCLUDE_DIR="$grpc_root/include" \
  -DgRPC_DIR="$grpc_root/lib/x86_64-linux-gnu/cmake/grpc" \
  -DProtobuf_INCLUDE_DIR="$grpc_root/include" \
  -DProtobuf_LIBRARY="$grpc_root/lib/x86_64-linux-gnu/libprotobuf.so" \
  -DProtobuf_PROTOC_EXECUTABLE="$grpc_root/bin/protoc" \
  -DPKG_CONFIG_EXECUTABLE="$grpc_root/bin/pkg-config" \
  -Dnlohmann_json_DIR="$cpp_deps/share/cmake/nlohmann_json" \
  -Dredis++_DIR="$cpp_install/share/cmake/redis++" \
  -Dhiredis_DIR="$cpp_install/lib/cmake/hiredis"
cmake --build /tmp/c17-installed-current --parallel 2

export LD_LIBRARY_PATH="/tmp/c-current-prefix/lib:$grpc_root/lib/x86_64-linux-gnu:$cpp_install/lib:$cpp_deps/lib/x86_64-linux-gnu:$openssl_runtime/lib/x86_64-linux-gnu:/usr/lib/x86_64-linux-gnu"
sha256sum \
  /workspace/sdk/c/src/dilock_c.cpp \
  /workspace/sdk/c/include/dilock/dilock.h \
  /workspace/sdk/c/tests/installed_consumer/main.c \
  /workspace/sdk/c/tests/installed_consumer/etcd_main.c \
  /workspace/sdk/c/tests/installed_consumer/consul_main.c \
  /workspace/sdk/c/tests/installed_consumer/CMakeLists.txt \
  /workspace/sdk/cpp/CMakeLists.txt \
  /workspace/sdk/c/tests/installed_consumer/run-docker.sh
cmake --version
cc --version | head -n 1
c++ --version | head -n 1
"$grpc_root/bin/protoc" --version
ldd /tmp/c17-installed-current/dilock_installed_c17_consumer
ldd /tmp/c17-installed-current/dilock_installed_c17_etcd_consumer
/tmp/c17-installed-current/dilock_installed_c17_consumer
/tmp/c17-installed-current/dilock_installed_c17_etcd_consumer
