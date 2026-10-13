CARGO ?= cargo
MANIFEST = rust-protocol/Cargo.toml

.PHONY: build test verify examples check
build:
	$(CARGO) build --release --locked --manifest-path $(MANIFEST)

test:
	$(CARGO) test --locked --manifest-path $(MANIFEST)

verify: build
	./rust-protocol/target/release/verify

examples: build
	mkdir -p evidence/examples
# Top-level JSON files are Rust fixtures; host adapter inputs live in examples/host/.
	@for spec in examples/*.json; do ./rust-protocol/target/release/mas demo "$$spec" "evidence/examples/$$(basename "$$spec" .json).json" >/dev/null || exit $$?; done
	$(CARGO) run --quiet --release --locked --manifest-path $(MANIFEST) --example recovery > evidence/examples/recovery.json
	$(CARGO) run --quiet --release --locked --manifest-path $(MANIFEST) --example gateway > evidence/examples/gateway.json

check: test verify examples
