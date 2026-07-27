.PHONY: \
	app \
	appimage \
	doc-check \
	docs-build \
	docs-serve \
	docs-test \
	fmt \
	fmt-check \
	fuzz \
	fuzz-check \
	lint \
	test \
	test-all \
	pre-commit

FUZZ_TARGETS := fuzz_pack_scan fuzz_decompress fuzz_msgpack_snapshot_meta \
	fuzz_msgpack_index_blob fuzz_item_stream fuzz_file_cache_decode fuzz_unpack_object

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

lint:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
	cargo test --workspace

test-all:
	cargo test --workspace -- --include-ignored

doc-check:
	RUSTDOCFLAGS="-Dwarnings" cargo doc --workspace --no-deps --all-features

pre-commit: fmt-check lint doc-check test-all

APP_BUNDLE = target/release/Vykar Backup.app

appimage:
	cargo build --release -p vykar-gui
	scripts/build-appimage.sh target/release/vykar-gui dist/

app:
	cargo build --release -p vykar-gui
	crates/vykar-gui/macos/create-icns.sh docs/src/images/logo-colored-gradient.svg target/AppIcon.icns
	mkdir -p "$(APP_BUNDLE)/Contents/MacOS" "$(APP_BUNDLE)/Contents/Resources"
	cp crates/vykar-gui/macos/Info.plist "$(APP_BUNDLE)/Contents/"
	cp target/release/vykar-gui "$(APP_BUNDLE)/Contents/MacOS/"
	cp target/AppIcon.icns "$(APP_BUNDLE)/Contents/Resources/"

fuzz:
	@for target in $(FUZZ_TARGETS); do \
		echo "==> Fuzzing $$target (60s)"; \
		cargo +nightly fuzz run $$target -- \
			-max_total_time=60 -rss_limit_mb=4096 -max_len=1048576 || exit 1; \
	done

fuzz-check:
	@for target in $(FUZZ_TARGETS); do \
		echo "==> Replaying corpus for $$target"; \
		cargo +nightly fuzz run $$target -- \
			-runs=0 -rss_limit_mb=4096 -max_len=1048576 || exit 1; \
	done

docs-build:
	mdbook build docs

docs-serve:
	mdbook serve docs --open

docs-test:
	mdbook test docs
