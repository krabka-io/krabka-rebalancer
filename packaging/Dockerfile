FROM rust:1.98.1-bookworm AS build
RUN apt-get update && apt-get install --yes --no-install-recommends clang cmake ninja-build \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --locked --release -p crabka-rebalancer --bin crabka-rebalancer

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/crabka-rebalancer /usr/bin/crabka-rebalancer
ENTRYPOINT ["/usr/bin/crabka-rebalancer"]
