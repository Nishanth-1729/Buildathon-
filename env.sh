# Source this file: everything (toolchains, caches, containers) lives inside this folder.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
export RUSTUP_HOME="$ROOT/.tools/rustup" CARGO_HOME="$ROOT/.tools/cargo"
export UV_INSTALL_DIR="$ROOT/.tools/bin" UV_PYTHON_INSTALL_DIR="$ROOT/.tools/python" UV_CACHE_DIR="$ROOT/.tools/uv-cache"
export UV_NO_MODIFY_PATH=1 RUSTUP_INIT_SKIP_PATH_CHECK=yes
export PATH="$ROOT/.tools/bin:$CARGO_HOME/bin:$ROOT/.venv/bin:$ROOT/.tools/git/bin:$PATH"
export UV_PYTHON_BIN_DIR="$ROOT/.tools/bin"
