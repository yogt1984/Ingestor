FROM rust:slim

# Install dependencies (OpenSSL dev & pkg-config)
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    ca-certificates \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY . .

RUN cargo build --release

CMD ["./target/release/binance_ingestor"]
