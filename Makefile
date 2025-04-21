

start:
	cargo build && RUST_LOG=info cargo run

debug:
	cargo build && RUST_LOG=debug cargo run

test:
	cargo test

compose:
	docker compose up --build

