FROM rust:1.88-bookworm AS build

WORKDIR /app
COPY . .
RUN cargo build --release -p archive-server -p archive-agent

FROM debian:bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl \
  && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/target/release/archive-server /usr/local/bin/archive-server
COPY --from=build /app/target/release/archive-agent /usr/local/bin/archive-agent

EXPOSE 8787
CMD ["archive-server"]
