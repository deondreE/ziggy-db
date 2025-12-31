const std = @import("std");

// Although this function looks imperative, it does not perform the build
// directly and instead it mutates the build graph (`b`) that will be then
// executed by an external runner. The functions in `std.Build` implement a DSL
// for defining build steps and express dependencies between them, allowing the
// build runner to parallelize the build automatically (and the cache system to
// know when a step doesn't need to be re-run).
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("testing_zig", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
    });

    const exe = b.addExecutable(.{
        .name = "ziggy_db",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/shell.zig"),

            .target = target,
            .optimize = optimize,

            .imports = &.{.{ .name = "testing_zig", .module = mod }},
        }),
    });

    const exe_2 = b.addExecutable(.{
        .name = "ziggy_cold",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/cold/shell.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "testing_zig", .module = mod }},
        }),
    });

    b.installArtifact(exe);
    b.installArtifact(exe_2);

    const bindings_lib = b.addLibrary(.{
        .name = "ziggy_bindings",
        .linkage = .dynamic,
        .version = .{ .major = 0, .minor = 0, .patch = 1 },
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bindings.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });

    _ = bindings_lib.getEmittedH();
    b.installArtifact(bindings_lib);

    const is_release =
        optimize == .ReleaseSafe or
        optimize == .ReleaseFast or
        optimize == .ReleaseSmall;

    if (is_release) {
        // TODO: Convert the man pages to HTML for windows context.
        const target_info = b.graph.host;
        const is_windows = target_info.result.os.tag == .windows;

        if (is_windows) {
            b.installDirectory(.{
                .source_dir = b.path("docs/man"),
                .install_dir = .prefix,
                .install_subdir = "share/docs/ziggydb", // easier to open in explorer.
            });
        } else {
            b.installDirectory(.{
                .source_dir = b.path("docs/man/"),
                .install_dir = .prefix,
                .install_subdir = "share/man/man1",
            });
        }
    }

    const run_step = b.step("run", "Run the app");

    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const mod_tests = b.addTest(.{
        .root_module = mod,
    });
    const run_mod_tests = b.addRunArtifact(mod_tests);

    const exe_tests = b.addTest(.{
        .root_module = exe.root_module,
    });

    const run_exe_tests = b.addRunArtifact(exe_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_exe_tests.step);
}
