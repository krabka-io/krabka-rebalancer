FROM rust:1.98.1-bookworm AS build
RUN apt-get update && apt-get install --yes --no-install-recommends clang cmake ninja-build \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --locked --release -p krabka-rebalancer --bin krabka-rebalancer

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/krabka-rebalancer /usr/bin/krabka-rebalancer
ENTRYPOINT ["/usr/bin/krabka-rebalancer"]
