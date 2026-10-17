// swift-tools-version: 6.1
import PackageDescription

// SwiftPM resolves Git dependencies from the repository root. Keep the
// implementation under sdk/swift while exposing its public products here.
let package = Package(
    name: "Dilock",
    platforms: [.macOS(.v13)],
    products: [
        .library(name: "DilockCore", targets: ["DilockCore"]),
        .library(name: "DilockRedis", targets: ["DilockRedis"]),
        .library(name: "DilockEtcd", targets: ["DilockEtcd"]),
        .library(name: "DilockConsul", targets: ["DilockConsul"]),
        .library(name: "DilockSwiftWorkerSupport", targets: ["DilockSwiftWorkerSupport"]),
        .executable(name: "dilock-swift-worker", targets: ["DilockSwiftWorker"]),
    ],
    dependencies: [
        .package(url: "https://github.com/swift-server/RediStack.git", exact: "1.6.3"),
        .package(url: "https://github.com/grpc/grpc-swift-2.git", exact: "2.0.0"),
        .package(url: "https://github.com/grpc/grpc-swift-nio-transport.git", from: "2.0.0"),
        .package(url: "https://github.com/grpc/grpc-swift-protobuf.git", from: "2.1.1"),
        .package(url: "https://github.com/apple/swift-protobuf.git", from: "1.38.1"),
        .package(url: "https://github.com/apple/swift-crypto.git", from: "3.12.0"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.0.0"),
    ],
    targets: [
        .target(
            name: "DilockCore",
            dependencies: [.product(name: "Crypto", package: "swift-crypto")],
            path: "sdk/swift/Sources/DilockCore"
        ),
        .target(
            name: "DilockRedis",
            dependencies: [
                "DilockCore",
                .product(name: "RediStack", package: "RediStack"),
                .product(name: "NIOCore", package: "swift-nio"),
            ],
            path: "sdk/swift/Sources/DilockRedis",
            resources: [.process("Resources")]
        ),
        .target(
            name: "EtcdProtos",
            dependencies: [
                .product(name: "GRPCCore", package: "grpc-swift-2"),
                .product(name: "SwiftProtobuf", package: "swift-protobuf"),
                .product(name: "GRPCProtobuf", package: "grpc-swift-protobuf"),
            ],
            path: "sdk/swift/Sources/EtcdProtos",
            exclude: [
                "UPSTREAM.md", "LICENSE-etcd-api", "LICENSE-etcd-server",
                "rpc.proto", "kv.proto", "v3lock.proto",
                "grpc-swift-proto-generator-config.json", "swift-protobuf-config.json",
            ]
        ),
        .target(
            name: "DilockEtcd",
            dependencies: [
                "DilockCore", "EtcdProtos",
                .product(name: "GRPCCore", package: "grpc-swift-2"),
                .product(name: "GRPCNIOTransportHTTP2", package: "grpc-swift-nio-transport"),
                .product(name: "GRPCProtobuf", package: "grpc-swift-protobuf"),
            ],
            path: "sdk/swift/Sources/DilockEtcd"
        ),
        .target(name: "DilockConsul", dependencies: ["DilockCore"], path: "sdk/swift/Sources/DilockConsul"),
        .executableTarget(
            name: "DilockSwiftWorker",
            dependencies: [
                "DilockCore", "DilockRedis", "DilockEtcd", "DilockSwiftWorkerSupport",
                .product(name: "RediStack", package: "RediStack"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
            ],
            path: "sdk/swift/Sources/DilockSwiftWorker"
        ),
        .target(name: "DilockSwiftWorkerSupport", path: "sdk/swift/Sources/DilockSwiftWorkerSupport"),
        .testTarget(
            name: "DilockCoreTests",
            dependencies: ["DilockCore"],
            path: "sdk/swift/Tests/DilockCoreTests",
            resources: [.process("Fixtures")]
        ),
        .testTarget(
            name: "DilockRedisTests",
            dependencies: [
                "DilockCore", "DilockRedis",
                .product(name: "RediStack", package: "RediStack"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
            ],
            path: "sdk/swift/Tests/DilockRedisTests"
        ),
        .testTarget(
            name: "DilockEtcdTests",
            dependencies: ["DilockCore", "DilockEtcd"],
            path: "sdk/swift/Tests/DilockEtcdTests"
        ),
        .testTarget(name: "DilockConsulTests", dependencies: ["DilockCore", "DilockConsul"], path: "sdk/swift/Tests/DilockConsulTests"),
        .testTarget(
            name: "DilockSwiftWorkerTests",
            dependencies: ["DilockSwiftWorkerSupport"],
            path: "sdk/swift/Tests/DilockSwiftWorkerTests"
        ),
    ]
)
