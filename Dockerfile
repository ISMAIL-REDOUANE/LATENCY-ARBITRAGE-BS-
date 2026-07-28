FROM rust:1.80-slim AS builder

WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/xchain_arb_bot .
COPY --from=builder /app/static /static

EXPOSE 8080

ENTRYPOINT ["./xchain_arb_bot"]
