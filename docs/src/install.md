# Installing

## Quick install

```bash
curl -fsSL https://vykar.borgbase.com/install.sh | sh
```

Or download the latest release for your platform from the [releases page](https://github.com/borgbase/vykar/releases).


## Pre-built binaries

Extract the archive and place the `vykar` binary somewhere on your `PATH`:

```bash
# Example for Linux/macOS
tar xzf vykar-*.tar.gz
sudo cp vykar /usr/local/bin/
```

For Windows CLI releases:

```powershell
Expand-Archive vykar-*.zip -DestinationPath .
Move-Item .\vykar.exe "$env:USERPROFILE\\bin\\vykar.exe"
```

Add your chosen directory (for example, `%USERPROFILE%\bin`) to `PATH` if needed.


## Build from source

Requires Rust 1.88 or later.

```bash
git clone https://github.com/borgbase/vykar.git
cd vykar
cargo build --release
```

The binary is at `target/release/vykar`. Copy it to a directory on your `PATH`:

```bash
cp target/release/vykar /usr/local/bin/
```

## Verify installation

```bash
vykar --version
```

## Next steps

- [Initialize and Set Up a Repository](init-setup.md)
