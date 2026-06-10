# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.91
ARG RUSTFLAGS="-Awarnings"
ARG ORTOOLS_VERSION=9.15.6755

FROM --platform=$BUILDPLATFORM rust:${RUST_VERSION}-trixie AS chef-base
WORKDIR /app
RUN cargo install cargo-chef

FROM chef-base AS planner
COPY Cargo.toml Cargo.lock ./
COPY ksolver/Cargo.toml ./ksolver/Cargo.toml
COPY vendor/cp_sat/Cargo.toml ./vendor/cp_sat/Cargo.toml
RUN mkdir -p ksolver/src vendor/cp_sat/src && \
    touch ksolver/src/lib.rs vendor/cp_sat/src/lib.rs
RUN cargo chef prepare --recipe-path recipe.json

FROM chef-base AS builder

ARG TARGETARCH
ARG RUSTFLAGS
ARG ORTOOLS_VERSION
ENV RUSTFLAGS=${RUSTFLAGS}
WORKDIR /app

ARG PROFILE_FLAG="--release"
ARG PROFILE_DIR=release

RUN dpkg --add-architecture amd64 && \
    dpkg --add-architecture arm64 && \
    apt-get update && DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-recommends \
    pkg-config ca-certificates make \
    gcc g++ \
    gcc-x86-64-linux-gnu g++-x86-64-linux-gnu \
    gcc-aarch64-linux-gnu g++-aarch64-linux-gnu \
    libc6-dev-amd64-cross libc6-dev-arm64-cross \
    libssl-dev libssl-dev:amd64 libssl-dev:arm64 \
    mold protobuf-compiler curl tar && \
    rm -rf /var/lib/apt/lists/*

ENV RUSTFLAGS="${RUSTFLAGS} -C link-arg=-fuse-ld=mold"

RUN case "${TARGETARCH}" in \
      amd64) \
        rustup target add x86_64-unknown-linux-gnu && \
        printf "%s\n" \
          'export SYSLENS_RUST_TARGET=x86_64-unknown-linux-gnu' \
          'export CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc' \
          'export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc' \
          'export PKG_CONFIG_ALLOW_CROSS=1' \
          'export X86_64_UNKNOWN_LINUX_GNU_OPENSSL_INCLUDE_DIR=/usr/include/x86_64-linux-gnu' \
          'export X86_64_UNKNOWN_LINUX_GNU_OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu' \
          > /tmp/rust-env \
        ;; \
      arm64) \
        rustup target add aarch64-unknown-linux-gnu && \
        printf "%s\n" \
          'export SYSLENS_RUST_TARGET=aarch64-unknown-linux-gnu' \
          'export CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc' \
          'export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc' \
          'export PKG_CONFIG_ALLOW_CROSS=1' \
          'export AARCH64_UNKNOWN_LINUX_GNU_OPENSSL_INCLUDE_DIR=/usr/include/aarch64-linux-gnu' \
          'export AARCH64_UNKNOWN_LINUX_GNU_OPENSSL_LIB_DIR=/usr/lib/aarch64-linux-gnu' \
          > /tmp/rust-env \
        ;; \
      *) echo "Unsupported TARGETARCH: ${TARGETARCH}" >&2 && exit 1 ;; \
    esac

RUN case "${TARGETARCH}" in \
      amd64) ORTOOLS_ARCHIVE="or-tools_amd64_ubuntu-24.04_cpp_v${ORTOOLS_VERSION}.tar.gz" ;; \
      arm64) ORTOOLS_ARCHIVE="or-tools_aarch64_AlmaLinux-8.10_cpp_v${ORTOOLS_VERSION}.tar.gz" ;; \
      *) echo "Unsupported TARGETARCH: ${TARGETARCH}" >&2 && exit 1 ;; \
    esac && \
    curl -fsSL "https://github.com/google/or-tools/releases/download/v9.15/${ORTOOLS_ARCHIVE}" -o /tmp/ortools.tar.gz && \
    mkdir -p /opt/ortools && \
    tar -xzf /tmp/ortools.tar.gz -C /opt/ortools --strip-components=1 && \
    mkdir -p /opt/ortools/lib && \
    if [ -d /opt/ortools/lib64 ]; then \
      ln -sf /opt/ortools/lib64/libortools.so /opt/ortools/lib/libortools.so && \
      ln -sf /opt/ortools/lib64/libortools.so.9 /opt/ortools/lib/libortools.so.9 && \
      ln -sf /opt/ortools/lib64/libortools.so.9.15.6755 /opt/ortools/lib/libortools.so.9.15.6755 && \
      ln -sf /opt/ortools/lib64/libortools_flatzinc.so /opt/ortools/lib/libortools_flatzinc.so && \
      ln -sf /opt/ortools/lib64/libortools_flatzinc.so.9 /opt/ortools/lib/libortools_flatzinc.so.9 && \
      ln -sf /opt/ortools/lib64/libortools_flatzinc.so.9.15.6755 /opt/ortools/lib/libortools_flatzinc.so.9.15.6755; \
    fi && \
    rm -f /tmp/ortools.tar.gz

ENV ORTOOLS_PREFIX=/opt/ortools
ENV CXXFLAGS="-I/opt/ortools/include"

RUN cp -aL /opt/ortools/lib64/*.so* /opt/ortools/lib/ 2>/dev/null; \
    echo "/opt/ortools/lib" > /etc/ld.so.conf.d/ortools.conf && ldconfig

COPY --from=planner /app/recipe.json recipe.json
COPY vendor/cp_sat/Cargo.toml ./vendor/cp_sat/Cargo.toml
RUN mkdir -p vendor/cp_sat/src && touch vendor/cp_sat/src/lib.rs

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-${TARGETARCH} \
    --mount=type=cache,target=/usr/local/cargo/git,id=cargo-git-${TARGETARCH} \
    --mount=type=cache,target=/app/target,sharing=locked,id=ksolver-target-${TARGETARCH} \
    . /tmp/rust-env && \
    export LDFLAGS="-L/opt/ortools/lib -L/opt/ortools/lib64 -L/usr/lib -L/usr/lib/x86_64-linux-gnu -L/usr/lib/aarch64-linux-gnu" && \
    cargo chef cook $PROFILE_FLAG --recipe-path recipe.json --package ksolver --features rust-cp-sat --target "$SYSLENS_RUST_TARGET"

COPY Cargo.toml Cargo.lock ./
COPY ksolver ./ksolver
COPY vendor/cp_sat ./vendor/cp_sat

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-${TARGETARCH} \
    --mount=type=cache,target=/usr/local/cargo/git,id=cargo-git-${TARGETARCH} \
    --mount=type=cache,target=/app/target,sharing=locked,id=ksolver-target-${TARGETARCH} \
    . /tmp/rust-env && \
    export LDFLAGS="-L/opt/ortools/lib -L/opt/ortools/lib64 -L/usr/lib -L/usr/lib/x86_64-linux-gnu -L/usr/lib/aarch64-linux-gnu" && \
    cargo build --package ksolver --features rust-cp-sat --target "$SYSLENS_RUST_TARGET" $PROFILE_FLAG && \
    cp -v /app/target/"$SYSLENS_RUST_TARGET"/$PROFILE_DIR/ksolver /tmp/ksolver-binary

RUN cp -v /tmp/ksolver-binary /usr/local/bin/ksolver && \
    chmod +x /usr/local/bin/ksolver

FROM --platform=$TARGETPLATFORM debian:trixie-slim AS runtime
WORKDIR /app

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN --mount=from=builder,source=/opt/ortools/,target=/mnt/ortools \
    for dir in /mnt/ortools/lib /mnt/ortools/lib64; do \
      [ -d "$dir" ] && find "$dir" -maxdepth 1 -name '*.so*' -exec cp -L {} /usr/local/lib/ \; ; \
    done; \
    ldconfig

COPY --from=builder /usr/local/bin/ksolver /usr/local/bin/ksolver

RUN mkdir -p /tmp && chmod 1777 /tmp

USER 65532:65532

ENTRYPOINT ["/usr/local/bin/ksolver"]
CMD ["serve"]
