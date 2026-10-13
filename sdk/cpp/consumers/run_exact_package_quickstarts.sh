#!/usr/bin/env bash
set -euo pipefail

repo="${1:-/workspace}"
redis_root="$repo/.tools/cppinstall"
grpc_root="$repo/.tools/grpc-root/usr"
deps_root="$repo/.tools/cppdeps/usr"
openssl_root="$repo/.tools/openssl/usr"
openssl_runtime="$repo/.tools/openssl-runtime/usr"
prefixes="$grpc_root;$redis_root;$deps_root;$openssl_root;$openssl_runtime"
scratch="$(mktemp -d /tmp/dilock-cpp-exact-package.XXXXXX)"
trap 'rm -rf "$scratch"' EXIT

export PATH="$grpc_root/bin:$PATH"
export LD_LIBRARY_PATH="$grpc_root/lib/x86_64-linux-gnu:$redis_root/lib:$deps_root/lib/x86_64-linux-gnu:$openssl_runtime/lib/x86_64-linux-gnu"
export PKG_CONFIG_SYSROOT_DIR="$repo/.tools/grpc-root"
export PKG_CONFIG_PATH="$grpc_root/lib/x86_64-linux-gnu/pkgconfig"
export PKG_CONFIG_LIBDIR="$PKG_CONFIG_PATH"

cmake_dependencies=(
    "-DCMAKE_PREFIX_PATH=$prefixes"
    "-DOpenSSL_ROOT_DIR=$openssl_root"
    "-DOpenSSL_INCLUDE_DIR=$openssl_root/include"
    "-DOPENSSL_CRYPTO_LIBRARY=$openssl_runtime/lib/x86_64-linux-gnu/libcrypto.so.3"
    "-DOPENSSL_SSL_LIBRARY=$openssl_runtime/lib/x86_64-linux-gnu/libssl.so.3"
    -DOPENSSL_USE_STATIC_LIBS=FALSE
    "-DgRPC_DIR=$grpc_root/lib/x86_64-linux-gnu/cmake/grpc"
    "-DProtobuf_INCLUDE_DIR=$grpc_root/include"
    "-DProtobuf_LIBRARY=$grpc_root/lib/x86_64-linux-gnu/libprotobuf.so"
    "-DProtobuf_PROTOC_EXECUTABLE=$grpc_root/bin/protoc"
    "-Dnlohmann_json_DIR=$deps_root/share/cmake/nlohmann_json"
    "-Dredis++_DIR=$redis_root/share/cmake/redis++"
    "-DHiredis_DIR=$redis_root/lib/cmake/hiredis"
)

cmake -S "$repo/sdk/cpp" -B "$scratch/package-build" \
    -DCMAKE_BUILD_TYPE=Release -DDILOCK_BUILD_TESTS=OFF -DDILOCK_BUILD_ETCD=ON \
    "-DCMAKE_INSTALL_PREFIX=$scratch/stage" "${cmake_dependencies[@]}"
cmake --build "$scratch/package-build" --parallel 2
cmake --install "$scratch/package-build"

mkdir "$scratch/archive-root" "$scratch/extracted"
cp -a "$scratch/stage" "$scratch/archive-root/prefix"
tar -czf "$scratch/dilock-cpp-current.tar.gz" -C "$scratch/archive-root" prefix
tar -xzf "$scratch/dilock-cpp-current.tar.gz" -C "$scratch/extracted"
installed="$scratch/extracted/prefix"
printf 'EXACT_PACKAGE_SHA256='
sha256sum "$scratch/dilock-cpp-current.tar.gz"

configure_consumer() {
    local source="$1" build="$2"
    cmake -S "$source" -B "$build" -DCMAKE_BUILD_TYPE=Release \
        "-DCMAKE_PREFIX_PATH=$installed;$prefixes" \
        "-DDilockCpp_DIR=$installed/lib/cmake/DilockCpp" "${cmake_dependencies[@]:1}"
    cmake --build "$build" --parallel 2
}

configure_consumer "$repo/sdk/cpp/consumers/cmake" "$scratch/external-consumer"
export DILOCK_REDIS_HOST=redis
export DILOCK_ETCD_ENDPOINT=etcd:2379
export DILOCK_NAMESPACE=default
export DILOCK_EPOCH=cpp-consumer-epoch-20261008
"$scratch/external-consumer/dilock_cpp_artifact_consumer"

configure_consumer "$repo/docs/examples/cpp-redis/quickstart" "$scratch/redis-quickstart"
configure_consumer "$repo/docs/examples/cpp-etcd/quickstart" "$scratch/etcd-quickstart"

# Minimal mode uses default namespace and policy. Configured mode consumes the
# separately provisioned namespace and explicit policy; shared mode holds two
# distinct resources concurrently through one provider and transport.
"$scratch/redis-quickstart/dilock_redis_quickstart"
"$scratch/etcd-quickstart/dilock_etcd_quickstart"
export DILOCK_NAMESPACE=cpp-consumer-20261008
"$scratch/redis-quickstart/dilock_redis_quickstart" --configured
"$scratch/etcd-quickstart/dilock_etcd_quickstart" --configured
export DILOCK_NAMESPACE=default
"$scratch/redis-quickstart/dilock_redis_quickstart" --shared-provider
"$scratch/etcd-quickstart/dilock_etcd_quickstart" --shared-provider

printf 'PASS exact current-source dual-backend installed package consumer\n'
printf 'PASS minimum, configured, shared-provider Redis and etcd quickstarts (6/6)\n'
