// swift-tools-version: 6.1
import PackageDescription

let package = Package(
    name: "DilockRedisQuickstart",
    dependencies: [
        .package(path: "../../../../sdk/swift"),
        .package(url: "https://github.com/swift-server/RediStack.git", exact: "1.6.3"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.0.0"),
    ],
    targets: [
        .executableTarget(
            name: "DilockRedisQuickstart",
            dependencies: [
                .product(name: "DilockCore", package: "Dilock"),
                .product(name: "DilockRedis", package: "Dilock"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "RediStack", package: "RediStack"),
            ],
            path: ".",
            exclude: ["Package.swift", "README.md"]
        )
    ]
)
