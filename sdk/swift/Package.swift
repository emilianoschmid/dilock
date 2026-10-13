// swift-tools-version: 6.1
import PackageDescription

let package = Package(
    name: "Dilock",
    platforms: [.macOS(.v13)],
    products: [
        .library(name: "DilockCore", targets: ["DilockCore"]),
        .library(name: "DilockRedis", targets: ["DilockRedis"]),
        .library(name: "DilockEtcd", targets: ["DilockEtcd"]),
        .library(name: "DilockSwiftWorkerSupport", targets: ["DilockSwiftWorkerSupport"]),
        .executable(name: "dilock-swift-worker", targets: ["DilockSwiftWorker"]),
    ],
    dependencies: [
        // Stable upstream versions confirmed from their release metadata for this task.
        .package(url: "https://github.com/swift-server/RediStack.git", exact: "1.6.3"),
        .package(url: "https://github.com/grpc/grpc-swift-2.git", exact: "2.0.0"),
        .package(url: "https://github.com/grpc/grpc-swift-nio-transport.git", from: "2.0.0"),
        .package(url: "https://github.com/grpc/grpc-swift-protobuf.git", from: "2.1.1"),
        .package(url: "https://github.com/apple/swift-protobuf.git", from: "1.38.1"),
        .package(url: "https://github.com/apple/swift-crypto.git", from: "3.12.0"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.0.0"),
    ],
    targets: [
        .target(name: "DilockCore", dependencies: [.product(name: "Crypto", package: "swift-crypto")]),
        .target(
            name: "DilockRedis",
            dependencies: [
                "DilockCore", .product(name: "RediStack", package: "RediStack"),
                .product(name: "NIOCore", package: "swift-nio"),
            ],
            resources: [.process("Resources")]
        ),
        .target(
            name: "EtcdProtos",
            dependencies: [
                .product(name: "GRPCCore", package: "grpc-swift-2"),
                .product(name: "SwiftProtobuf", package: "swift-protobuf"),
                .product(name: "GRPCProtobuf", package: "grpc-swift-protobuf"),
            ],
            path: "Sources/EtcdProtos",
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
            ]
        ),
        .executableTarget(
            name: "DilockSwiftWorker",
            dependencies: [
                "DilockCore", "DilockRedis", "DilockEtcd", "DilockSwiftWorkerSupport",
                .product(name: "RediStack", package: "RediStack"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
            ]
        ),
        .target(name: "DilockSwiftWorkerSupport"),
        .testTarget(name: "DilockCoreTests", dependencies: ["DilockCore"], resources: [.process("Fixtures")]),
        .testTarget(
            name: "DilockRedisTests",
            dependencies: [
                "DilockCore", "DilockRedis",
                .product(name: "RediStack", package: "RediStack"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
            ]
        ),
        .testTarget(name: "DilockEtcdTests", dependencies: ["DilockCore", "DilockEtcd"]),
        .testTarget(name: "DilockSwiftWorkerTests", dependencies: ["DilockSwiftWorkerSupport"]),
    ]
)
