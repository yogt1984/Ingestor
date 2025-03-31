

start:
	cargo build && RUST_LOG=info cargo run

debug:
	cargo build && RUST_LOG=debug cargo run

compose:
	docker compose up --build

