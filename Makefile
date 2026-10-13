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
	@for spec in examples/*.json; do ./rust-protocol/target/release/mas demo "$$spec" "evidence/examples/$$(basename "$$spec" .json).json" >/dev/null || exit $$?; done

check: test verify examples
