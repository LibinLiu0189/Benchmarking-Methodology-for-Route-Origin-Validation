"""Copy local third-party sources and build BIRD, Bngblaster, and BRIO."""

import argparse
import os
from pathlib import Path
import shutil
import subprocess


# Execute a build command with visible output and stop on failure.
def run(command, directory, environment):
    print(f"Building in {directory}: {' '.join(map(str, command))}", flush=True)
    subprocess.run(list(map(str, command)), cwd=directory, env=environment, check=True)


# Compile independent source copies; retain any later edits to those copies.
def build(source_dir, output, jobs, sysroot=None, uthash_dir=None):
    source_dir = Path(source_dir).resolve()
    output = Path(output).resolve()
    if jobs < 1:
        raise ValueError("Jobs must be positive")
    sources = output / "src"
    sources.mkdir(parents=True, exist_ok=True)
    for name in ("bird", "bngblaster", "libdict", "brio"):
        target = sources / name
        if not target.exists():
            shutil.copytree(source_dir / name, target, symlinks=True)
    work = output / "build"
    binaries = output / "bin"
    work.mkdir(exist_ok=True)
    binaries.mkdir(exist_ok=True)
    dependencies = work / "deps"
    environment = os.environ.copy()
    for key in (
        "CFLAGS",
        "CXXFLAGS",
        "CPPFLAGS",
        "LDFLAGS",
        "CPATH",
        "C_INCLUDE_PATH",
        "CPLUS_INCLUDE_PATH",
        "LIBRARY_PATH",
        "LD_LIBRARY_PATH",
        "PKG_CONFIG_PATH",
        "CC",
        "CXX",
    ):
        environment.pop(key, None)
    environment.update(PATH="/usr/bin:/bin", CC="/usr/bin/gcc", CXX="/usr/bin/g++")
    includes = [dependencies / "include"]
    libraries = []
    if sysroot is not None:
        sysroot = Path(sysroot).resolve()
        # Copy build dependencies once so runtime does not depend on the old project.
        local_sysroot = output / "sysroot"
        if not local_sysroot.exists():
            shutil.copytree(sysroot, local_sysroot, symlinks=True)
        sysroot = local_sysroot
        library = sysroot / "usr/lib/x86_64-linux-gnu"
        includes.append(sysroot / "usr/include")
        libraries.append(library)
        environment.update(
            PATH=f"{sysroot / 'usr/bin'}:/usr/bin:/bin",
            LD_LIBRARY_PATH=str(library),
            PKG_CONFIG_PATH=str(library / "pkgconfig"),
            BISON_PKGDATADIR=str(sysroot / "usr/share/bison"),
        )
    if uthash_dir is not None:
        headers = output / "include"
        headers.mkdir(exist_ok=True)
        source_header = Path(uthash_dir).resolve() / "uthash.h"
        target_header = headers / "uthash.h"
        if source_header != target_header.resolve():
            shutil.copy2(source_header, target_header)
        includes.append(headers)
    environment["CPATH"] = os.pathsep.join(map(str, includes))
    include_flags = " ".join(f"-I{path}" for path in includes)
    link_flags = " ".join(f"-L{path} -Wl,-rpath,{path}" for path in libraries)
    if any(any(c.isspace() for c in str(path)) for path in includes + libraries):
        raise ValueError("Build dependency paths must not contain whitespace")

    bird_work = work / "bird"
    bird_work.mkdir(exist_ok=True)
    run(["autoreconf"], sources / "bird", environment)
    run(
        [
            sources / "bird/configure",
            "--disable-libssh",
            "--disable-debug",
            "CFLAGS=-O2 -g",
            f"CPPFLAGS={include_flags}",
            f"LDFLAGS={link_flags}",
        ],
        bird_work,
        environment,
    )
    run(["make", f"-j{jobs}"], bird_work, environment)

    run(
        [
            "cmake",
            "-S",
            sources / "libdict",
            "-B",
            work / "libdict",
            "-DCMAKE_BUILD_TYPE=Release",
            "-DLIBDICT_TESTS=OFF",
            "-DLIBDICT_TOOLS=OFF",
            "-DLIBDICT_SHARED=OFF",
            f"-DCMAKE_INSTALL_PREFIX={dependencies}",
        ],
        work,
        environment,
    )
    run(
        [
            "cmake",
            "--build",
            work / "libdict",
            "--target",
            "libdict-static",
            "-j",
            jobs,
        ],
        work,
        environment,
    )
    run(
        ["cmake", "--install", work / "libdict", "--component", "dev"],
        work,
        environment,
    )
    run(
        [
            "cmake",
            "-S",
            sources / "bngblaster",
            "-B",
            work / "bngblaster",
            "-DCMAKE_BUILD_TYPE=Release",
            "-DBNGBLASTER_VERSION=0.9.39",
            "-DGIT_REF=0.9.39",
            "-DGIT_SHA=local",
            f"-Dlibdict={dependencies / 'lib/libdict.a'}",
            f"-DCMAKE_EXE_LINKER_FLAGS={link_flags}",
            "-DCMAKE_C_FLAGS=",
            "-DBNGBLASTER_TESTS=OFF",
            "-DBNGBLASTER_DPDK=OFF",
            "-DBNGBLASTER_TIMER_LOGGING=OFF",
            "-DBNGBLASTER_CPU_NATIVE=OFF",
        ],
        work,
        environment,
    )
    run(
        ["cmake", "--build", work / "bngblaster", "--target", "bngblaster", "-j", jobs],
        work,
        environment,
    )

    brio = sources / "brio"
    io = brio / "bgprpki-io"
    files = [
        io / "rpki-rtr-protocol/rpkirtr_svr.c",
        *sorted((io / "srx-common").glob("*.c")),
        io / "antd-util/log.c",
        io / "antd-util/prefix.c",
    ]
    run(
        [
            "/usr/bin/gcc",
            "-O2",
            "-g",
            "-std=gnu11",
            "-DBRIO",
            f"-I{io}",
            f"-I{brio / 'srx-crypto-api'}",
            *(f"-I{path}" for path in includes),
            *files,
            *(f"-L{path}" for path in libraries),
            *(f"-Wl,-rpath,{path}" for path in libraries),
            "-lreadline",
            "-lpthread",
            "-lm",
            "-lrt",
            "-o",
            work / "brio_rc",
        ],
        work,
        environment,
    )
    for name, source in {
        "bird": bird_work / "bird",
        "birdc": bird_work / "birdc",
        "bngblaster": work / "bngblaster/code/bngblaster/bngblaster",
        "brio_rc": work / "brio_rc",
    }.items():
        shutil.copy2(source, binaries / name)
    print(f"Build complete: {binaries}")


# Parse source and dependency locations without installing system packages.
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("software/src"),
        help="Directory containing bird, bngblaster, libdict, and brio sources",
    )
    parser.add_argument("--output", type=Path, default=Path("software"))
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument(
        "--sysroot",
        type=Path,
        default=Path("software/sysroot"),
        help="Dependency sysroot (default: software/sysroot)",
    )
    parser.add_argument(
        "--uthash-dir",
        type=Path,
        default=Path("software/include"),
        help="Directory containing uthash.h (default: software/include)",
    )
    args = parser.parse_args()
    build(args.source_dir, args.output, args.jobs, args.sysroot, args.uthash_dir)


if __name__ == "__main__":
    main()
