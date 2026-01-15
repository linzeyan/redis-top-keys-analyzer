# 自動偵測 gstrip，沒有就用 brew 安裝 binutils，最後回傳 gstrip 路徑
STRIP ?= $(shell \
  if command -v gstrip >/dev/null 2>&1; then \
    command -v gstrip; \
  else \
    if ! brew list binutils >/dev/null 2>&1; then \
      brew install binutils >/dev/null 2>&1; \
    fi; \
    prefix=$$(brew --prefix binutils); \
    echo "$$prefix/bin/gstrip"; \
  fi \
)
CROSS_ENV ?= DOCKER_DEFAULT_PLATFORM=linux/amd64

.PHONY: all build fmt lint test clean bench

all: build build-x86_64 build-arm64

fmt:
	cargo fmt

lint:
	cargo clippy -- -D warnings

test:
	cargo test --all

build: fmt lint test
	@command -v cross >/dev/null || (cargo install cross)
	cargo build --release

# x86_64
build-x86_64:
	@rustup target list --installed | grep x86_64-unknown-linux-musl >/dev/null || (rustup target add x86_64-unknown-linux-musl)
	$(CROSS_ENV) cross build --release --target x86_64-unknown-linux-musl --target-dir target/cross-x86_64
# 縮小檔案
# 	strip target/cross-x86_64/x86_64-unknown-linux-musl/release/redis-top-keys-analyzer
	@command -v $(STRIP) >/dev/null || (brew install binutils)
	$(STRIP) target/cross-x86_64/x86_64-unknown-linux-musl/release/redis-top-keys-analyzer

# ARM64
build-arm64:
	@rustup target list --installed | grep aarch64-unknown-linux-musl >/dev/null || (rustup target add aarch64-unknown-linux-musl)
	$(CROSS_ENV) cross build --release --target aarch64-unknown-linux-musl --target-dir target/cross-aarch64
# 	strip target/cross-aarch64/aarch64-unknown-linux-musl/release/redis-top-keys-analyzer
	@command -v $(STRIP) >/dev/null || (brew install binutils)
	$(STRIP) target/cross-aarch64/aarch64-unknown-linux-musl/release/redis-top-keys-analyzer

clean:
	cargo clean

bench:
	cargo bench