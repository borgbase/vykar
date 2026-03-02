.PHONY: \
	app \
	docs-build \
	docs-serve \
	docs-test \
	fmt \
	fmt-check \
	lint \
	test \
	pre-commit

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

lint:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
	cargo test --workspace

pre-commit: fmt-check lint test

APP_BUNDLE = target/release/Vykar Backup.app

app:
	cargo build --release -p vykar-gui
	crates/vykar-gui/macos/create-icns.sh docs/src/images/logo-colored-gradient.svg target/AppIcon.icns
	mkdir -p "$(APP_BUNDLE)/Contents/MacOS" "$(APP_BUNDLE)/Contents/Resources"
	cp crates/vykar-gui/macos/Info.plist "$(APP_BUNDLE)/Contents/"
	cp target/release/vykar-gui "$(APP_BUNDLE)/Contents/MacOS/"
	cp target/AppIcon.icns "$(APP_BUNDLE)/Contents/Resources/"

docs-build:
	mdbook build docs

docs-serve:
	mdbook serve docs --open

docs-test:
	mdbook test docs
