// swift-tools-version: 6.1
import PackageDescription

let package = Package(
    name: "DilockEtcdQuickstart",
    dependencies: [.package(path: "../../../../sdk/swift")],
    targets: [
        .executableTarget(
            name: "DilockEtcdQuickstart",
            dependencies: [
                .product(name: "DilockCore", package: "Dilock"),
                .product(name: "DilockEtcd", package: "Dilock"),
            ],
            path: ".",
            exclude: ["Package.swift", "README.md"]
        )
    ]
)
