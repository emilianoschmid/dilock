$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "../../..")).Path
$image = "dilock-cpp-etcd-builder:20261006"

docker build --pull=false --tag $image --file (Join-Path $PSScriptRoot "Dockerfile.etcd-worker") $repoRoot
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

$build = @'
set -eu
export PATH=/workspace/.tools/grpc-root/usr/bin:$PATH
export LD_LIBRARY_PATH=/workspace/.tools/grpc-root/usr/lib/x86_64-linux-gnu:/workspace/.tools/cppinstall/lib:/workspace/.tools/cppdeps/usr/lib/x86_64-linux-gnu:/workspace/.tools/openssl-runtime/usr/lib/x86_64-linux-gnu
export PKG_CONFIG_SYSROOT_DIR=/workspace/.tools/grpc-root
export PKG_CONFIG_PATH=/workspace/.tools/grpc-root/usr/lib/x86_64-linux-gnu/pkgconfig
export PKG_CONFIG_LIBDIR=$PKG_CONFIG_PATH
cmake -S sdk/cpp -B .tools/dilock-cpp-etcd-docker-build \
  -DDILOCK_BUILD_ETCD=ON \
  -DDILOCK_BUILD_TESTS=OFF \
  -DCMAKE_PREFIX_PATH="/workspace/.tools/grpc-root/usr;/workspace/.tools/cppinstall;/workspace/.tools/cppdeps/usr" \
  -DOPENSSL_ROOT_DIR=/workspace/.tools/openssl/usr \
  -DOPENSSL_INCLUDE_DIR=/workspace/.tools/openssl/usr/include \
  -DOPENSSL_CRYPTO_LIBRARY=/workspace/.tools/openssl-runtime/usr/lib/x86_64-linux-gnu/libcrypto.so.3 \
  -DOPENSSL_SSL_LIBRARY=/workspace/.tools/openssl-runtime/usr/lib/x86_64-linux-gnu/libssl.so.3 \
  -DPKG_CONFIG_EXECUTABLE=/workspace/.tools/grpc-root/usr/bin/pkg-config
cmake --build .tools/dilock-cpp-etcd-docker-build --target dilock_cpp_worker --parallel 2
'@

docker run --rm --pull=never --user 0:0 `
    --mount "type=bind,source=$repoRoot,target=/workspace" `
    --workdir /workspace `
    $image /bin/sh -lc $build
exit $LASTEXITCODE
