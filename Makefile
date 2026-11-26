.PHONY: \
	app \
	appimage \
	boundaries \
	doc-check \
	docs-build \
	docs-serve \
	fmt \
	fmt-check \
	fuzz \
	fuzz-check \
	fuzz-list \
	lint \
	product-features \
	sim-campaign \
	test \
	test-all \
	pre-commit

FUZZ_TARGETS := $(sort $(patsubst fuzz/fuzz_targets/%.rs,%,$(wildcard fuzz/fuzz_targets/fuzz_*.rs)))
FUZZ_TARGETS_GUARD = $(if $(FUZZ_TARGETS),,$(error no fuzz targets found; run make from the repository root))

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

boundaries:
	sh scripts/check-module-boundaries.sh

product-features:
	sh scripts/check-product-features.sh

lint: boundaries
	cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
	cargo test --workspace

test-all:
	cargo test --workspace -- --include-ignored

doc-check:
	RUSTDOCFLAGS="-Dwarnings" cargo doc --workspace --no-deps --all-features

pre-commit: fmt-check lint doc-check product-features test-all

APP_BUNDLE = target/release/Vykar Backup.app

appimage:
	cargo build --release -p vykar-gui
	scripts/build-appimage.sh target/release/vykar-gui dist/

app:
	cargo build --release -p vykar-gui
	crates/vykar-gui/macos/create-icns.sh docs/public/images/logo-colored-gradient.svg target/AppIcon.icns
	mkdir -p "$(APP_BUNDLE)/Contents/MacOS" "$(APP_BUNDLE)/Contents/Resources"
	cp crates/vykar-gui/macos/Info.plist "$(APP_BUNDLE)/Contents/"
	cp target/release/vykar-gui "$(APP_BUNDLE)/Contents/MacOS/"
	cp target/AppIcon.icns "$(APP_BUNDLE)/Contents/Resources/"

fuzz:
	$(FUZZ_TARGETS_GUARD)
	@for target in $(FUZZ_TARGETS); do \
		echo "==> Fuzzing $$target (60s)"; \
		cargo +nightly fuzz run $$target -- \
			-max_total_time=60 -rss_limit_mb=4096 -max_len=1048576 || exit 1; \
	done

fuzz-check:
	$(FUZZ_TARGETS_GUARD)
	@for target in $(FUZZ_TARGETS); do \
		echo "==> Replaying corpus for $$target"; \
		cargo +nightly fuzz run $$target -- \
			-runs=0 -rss_limit_mb=4096 -max_len=1048576 || exit 1; \
	done

fuzz-list:
	$(FUZZ_TARGETS_GUARD)
	@echo $(FUZZ_TARGETS)

# Long protocol-simulation campaign (08 step 5): SIM_SECONDS of bounded
# searches and random cases. Failing traces, and each worker's case in
# progress, go to SIM_TRACE_DIR. Fails if any case shows damage of an
# unknown shape.
SIM_SECONDS ?= 3600
SIM_TRACE_DIR ?= $(CURDIR)/target/sim-campaign

sim-campaign:
	VYKAR_SIM_CAMPAIGN_SECS=$(SIM_SECONDS) VYKAR_SIM_TRACE_DIR="$(SIM_TRACE_DIR)" \
		cargo test -p vykar-core --lib tests::sim_campaign::campaign -- --exact --nocapture

docs-build:
	cd docs && pnpm install --frozen-lockfile && pnpm build

docs-serve:
	cd docs && pnpm install --frozen-lockfile && pnpm dev --open
