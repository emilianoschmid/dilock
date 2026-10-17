from conan import ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout


class DilockCppConan(ConanFile):
    name = "dilock-cpp"
    version = "0.1.0"
    license = "Apache-2.0"
    package_type = "library"
    settings = "os", "arch", "compiler", "build_type"
    options = {"with_etcd": [True, False], "with_consul": [True, False], "shared": [True, False]}
    default_options = {"with_etcd": False, "with_consul": False, "shared": False}
    exports_sources = "LICENSE", "CMakeLists.txt", "cmake/*", "consumer/*", "include/**", "proto/**", "resources/*", "src/*", "tests/*", "worker.cpp"

    def requirements(self):
        self.requires("redis-plus-plus/1.3.15")
        self.requires("openssl/3.5.8")
        self.requires("nlohmann_json/3.12.0")
        if self.options.with_consul:
            self.requires("libcurl/8.21.0")
        if self.options.with_etcd:
            self.requires("protobuf/3.21.12")
            self.requires("grpc/1.54.3")

    def layout(self):
        cmake_layout(self)

    def generate(self):
        CMakeDeps(self).generate()
        toolchain = CMakeToolchain(self)
        toolchain.variables["DILOCK_BUILD_TESTS"] = False
        toolchain.variables["DILOCK_BUILD_ETCD"] = bool(self.options.with_etcd)
        toolchain.variables["DILOCK_BUILD_CONSUL"] = bool(self.options.with_consul)
        toolchain.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        CMake(self).install()

    def package_info(self):
        self.cpp_info.includedirs = ["include"]
        self.cpp_info.set_property("cmake_file_name", "DilockCpp")
        redis = self.cpp_info.components["redis"]
        redis.libs = ["dilock_cpp"]
        redis.set_property("cmake_target_name", "Dilock::redis")
        redis.requires = [
            "openssl::crypto",
            "nlohmann_json::nlohmann_json",
            "redis-plus-plus::redis++lib",
        ]
        if self.options.with_etcd:
            etcd = self.cpp_info.components["etcd"]
            etcd.libs = ["dilock_cpp_etcd"]
            etcd.set_property("cmake_target_name", "Dilock::etcd")
            etcd.requires = [
                "openssl::crypto",
                "openssl::ssl",
                "nlohmann_json::nlohmann_json",
                "protobuf::libprotobuf",
                "grpc::grpc++",
            ]
        if self.options.with_consul:
            consul = self.cpp_info.components["consul"]
            consul.libs = ["dilock_cpp_consul"]
            consul.set_property("cmake_target_name", "Dilock::consul")
            consul.requires = ["redis", "libcurl::libcurl", "openssl::crypto", "nlohmann_json::nlohmann_json"]
