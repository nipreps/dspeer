# To create this Dockerfile, I used Claude to get an initial idea of which image I could use.
# References: https://docs.docker.com/reference/dockerfile/, https://docs.docker.com/build/building/multi-stage/
# https://hub.docker.com/_/rust, https://hub.docker.com/_/debian
# 1. Compilation

# Rust image: https://hub.docker.com/_/rust
# Stable version of Rust
# slim a lightweight variant of the official Rust image, sufficient for the testbed
FROM rust:1.96.0-slim-bookworm AS builder

# WORKDIR create the /app folder and changes the working directory to it for all commands
WORKDIR /app

# Copy all project file to /app inside the container
COPY . .

# --release Build with optimizations
# https://doc.rust-lang.org/cargo/commands/cargo-build.html
RUN cargo build --release

# 2. Final image

# References for the image: https://kerkour.com/rust-docker-small-secure-images
FROM debian:bookworm-slim AS runtime

WORKDIR /app

# - ca-certificates: TLS root certificates. Iroh contact its realy servers (hosted by n0) via HTTPS/QUIC
# - libssl3: OpenSSL library on which Rust depends
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/rust-mvp ./rust-mvp

# Create the data/ and cache/ directories reuqired by peer()
RUN mkdir -p data cache

# Specifies the command to be run when the container starts
ENTRYPOINT ["./rust-mvp"]