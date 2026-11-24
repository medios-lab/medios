# Dev environment for mesos-ai per DR-003 (#4): Docker-on-Mac → remote Linux.
#
# Image provides the build toolchain. The source tree is mounted from host
# (not copied) so edits on the host are reflected immediately in the container.
# Build and run inside the container; ship the resulting binaries to remote
# Linux for production-fidelity testing.
#
# Build:   docker build -t medios-dev .
# Run:     docker run --rm -it -v "$PWD":/workspace medios-dev
# Compose: docker compose up   (controller + node networked together)
#
# Per BUILD-008 (#72) the build expects CMake 3.24+; Ubuntu 24.04 LTS ships
# CMake 3.28. Per BUILD-010 (#76), OVERRIDE_FIND_PACKAGE handles boost +
# protobuf shadowing — system libs of those are intentionally NOT installed.

FROM ubuntu:24.04

# Avoid tzdata interactive prompt; install build deps non-interactively.
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -q && \
    apt-get install -y --no-install-recommends \
      build-essential \
      cmake \
      ninja-build \
      g++ \
      clang \
      ccache \
      git \
      ca-certificates \
      curl \
      libssl-dev \
      patchelf \
      pkg-config \
      rsync && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Create an unprivileged 'medios' developer account for non-root builds and
# unit-test runs (`docker run --user medios ...`); root remains available when
# running Linux cgroups/namespace isolator tests inside the container (#400).
RUN useradd --create-home --shell /bin/bash medios

# ccache wraps the compiler; build scripts pick it up via CMAKE_*_COMPILER_LAUNCHER.
RUN ccache --max-size=20G

# Source mounted from host at /workspace.
WORKDIR /workspace

# Default to an interactive shell; build commands run explicitly by the
# operator or by docker compose service entrypoints.
CMD ["bash"]
