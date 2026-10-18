const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const include_dir = b.option([]const u8, "c-include", "C ABI include directory") orelse "../../.local/dilock-c/include";
    const library_dir = b.option([]const u8, "c-lib", "C ABI library directory") orelse "../../.local/dilock-c/lib";
    const translate_c = b.addTranslateC(.{
        .root_source_file = b.path("src/c.h"),
        .target = target,
        .optimize = optimize,
    });
    translate_c.addIncludePath(.{ .cwd_relative = include_dir });
    const c_module = translate_c.createModule();

    // Register the facade as a public package module. `createModule` only
    // makes it available to targets in this build script; consumers resolve
    // this name through `dependency.module("dilock")`.
    const module = b.addModule("dilock", .{
        .root_source_file = b.path("src/dilock.zig"),
        .target = target,
        .optimize = optimize,
    });
    module.addImport("c", c_module);
    module.addIncludePath(.{ .cwd_relative = include_dir });
    module.addLibraryPath(.{ .cwd_relative = library_dir });
    module.linkSystemLibrary("dilock_c", .{});

    const library = b.addLibrary(.{
        .name = "dilock_zig",
        .root_module = module,
        .linkage = .static,
    });
    b.installArtifact(library);

    const redis_module = b.createModule(.{
        .root_source_file = b.path("examples/redis_quickstart.zig"),
        .target = target,
        .optimize = optimize,
    });
    redis_module.addImport("dilock", module);
    redis_module.addIncludePath(.{ .cwd_relative = include_dir });
    redis_module.addLibraryPath(.{ .cwd_relative = library_dir });
    redis_module.linkSystemLibrary("dilock_c", .{});
    b.installArtifact(b.addExecutable(.{ .name = "dilock-zig-redis-example", .root_module = redis_module }));

    const etcd_module = b.createModule(.{
        .root_source_file = b.path("examples/etcd_quickstart.zig"),
        .target = target,
        .optimize = optimize,
    });
    etcd_module.addImport("dilock", module);
    etcd_module.addIncludePath(.{ .cwd_relative = include_dir });
    etcd_module.addLibraryPath(.{ .cwd_relative = library_dir });
    etcd_module.linkSystemLibrary("dilock_c", .{});
    b.installArtifact(b.addExecutable(.{ .name = "dilock-zig-etcd-example", .root_module = etcd_module }));

    const consul_module = b.createModule(.{
        .root_source_file = b.path("examples/consul_quickstart.zig"),
        .target = target,
        .optimize = optimize,
    });
    consul_module.addImport("dilock", module);
    consul_module.addIncludePath(.{ .cwd_relative = include_dir });
    consul_module.addLibraryPath(.{ .cwd_relative = library_dir });
    consul_module.linkSystemLibrary("dilock_c", .{});
    b.installArtifact(b.addExecutable(.{ .name = "dilock-zig-consul-example", .root_module = consul_module }));

    const tests_module = b.createModule(.{
        .root_source_file = b.path("tests/contract.zig"),
        .target = target,
        .optimize = optimize,
    });
    tests_module.addImport("c", c_module);
    tests_module.addImport("dilock", module);
    tests_module.addIncludePath(.{ .cwd_relative = include_dir });
    tests_module.addLibraryPath(.{ .cwd_relative = library_dir });
    tests_module.linkSystemLibrary("dilock_c", .{});
    const tests = b.addTest(.{ .root_module = tests_module });
    const run_tests = b.addRunArtifact(tests);

    // Zig executes test declarations only from a test artifact's root module.
    // Importing `dilock` in tests/contract.zig compiles the facade but does not
    // run tests declared inside src/dilock.zig, so give the facade its own
    // test root as well.
    const facade_tests_module = b.createModule(.{
        .root_source_file = b.path("src/dilock.zig"),
        .target = target,
        .optimize = optimize,
    });
    facade_tests_module.addImport("c", c_module);
    facade_tests_module.addIncludePath(.{ .cwd_relative = include_dir });
    facade_tests_module.addLibraryPath(.{ .cwd_relative = library_dir });
    facade_tests_module.linkSystemLibrary("dilock_c", .{});
    const facade_tests = b.addTest(.{ .root_module = facade_tests_module });
    const run_facade_tests = b.addRunArtifact(facade_tests);

    const test_step = b.step("test", "Run ABI binding contract tests");
    test_step.dependOn(&run_tests.step);
    test_step.dependOn(&run_facade_tests.step);

    // Keep the PostgreSQL route independently linkable against the
    // PostgreSQL-only C ABI build. The combined contract root intentionally
    // exercises Redis, etcd and Consul null-handle guards, so it would require
    // their native symbols even when the live PostgreSQL test is the only
    // backend under evaluation.
    const postgres_tests_module = b.createModule(.{
        .root_source_file = b.path("tests/postgres.zig"),
        .target = target,
        .optimize = optimize,
    });
    postgres_tests_module.addImport("c", c_module);
    postgres_tests_module.addImport("dilock", module);
    postgres_tests_module.addIncludePath(.{ .cwd_relative = include_dir });
    postgres_tests_module.addLibraryPath(.{ .cwd_relative = library_dir });
    postgres_tests_module.linkSystemLibrary("dilock_c", .{});
    const postgres_tests = b.addTest(.{ .root_module = postgres_tests_module });
    const run_postgres_tests = b.addRunArtifact(postgres_tests);
    const postgres_test_step = b.step("test-postgres", "Run PostgreSQL ABI contract and live tests");
    postgres_test_step.dependOn(&run_postgres_tests.step);
}
