#!/usr/bin/env bash
# Download and install the Open Actions CLI binary.
# Usage: curl -fsSL https://raw.githubusercontent.com/kelos-dev/open-actions/main/hack/install.sh | bash

set -euo pipefail

REPO="kelos-dev/open-actions"
INSTALL_DIR="${INSTALL_DIR:-${HOME}/.local/bin}"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$ARCH" in
  x86_64) ARCH="amd64" ;;
  aarch64 | arm64) ARCH="arm64" ;;
  *)
    echo "Unsupported architecture: $ARCH" >&2
    exit 1
    ;;
esac

case "$OS" in
  linux | darwin) ;;
  *)
    echo "Unsupported OS: $OS" >&2
    exit 1
    ;;
esac

BINARY="open-actions-${OS}-${ARCH}"
BASE_URL="${OPEN_ACTIONS_RELEASE_URL:-https://github.com/${REPO}/releases/latest/download}"
URL="${BASE_URL}/${BINARY}"
CHECKSUMS_URL="${BASE_URL}/checksums.txt"

echo "Downloading ${BINARY}..."
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
BINARY_PATH="${TMP}/${BINARY}"
CHECKSUMS_PATH="${TMP}/checksums.txt"

if ! curl -fsSL -o "$BINARY_PATH" "$URL"; then
  echo "Failed to download ${URL}" >&2
  exit 1
fi

if ! curl -fsSL -o "$CHECKSUMS_PATH" "$CHECKSUMS_URL"; then
  echo "Failed to download ${CHECKSUMS_URL}" >&2
  exit 1
fi

EXPECTED_CHECKSUM="$(awk -v binary="$BINARY" '$2 == binary { print $1 }' "$CHECKSUMS_PATH")"
if [ -z "$EXPECTED_CHECKSUM" ]; then
  echo "Checksum not found for ${BINARY}" >&2
  exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
  ACTUAL_CHECKSUM="$(sha256sum "$BINARY_PATH" | awk '{ print $1 }')"
elif command -v shasum >/dev/null 2>&1; then
  ACTUAL_CHECKSUM="$(shasum -a 256 "$BINARY_PATH" | awk '{ print $1 }')"
else
  echo "Error: sha256sum or shasum is required" >&2
  exit 1
fi

if [ "$ACTUAL_CHECKSUM" != "$EXPECTED_CHECKSUM" ]; then
  echo "Checksum verification failed for ${BINARY}" >&2
  exit 1
fi

chmod +x "$BINARY_PATH"

if ! mkdir -p "$INSTALL_DIR" 2>/dev/null; then
  echo "Error: could not create ${INSTALL_DIR}" >&2
  echo "Set INSTALL_DIR to a writable path and try again" >&2
  exit 1
fi

if [ -w "$INSTALL_DIR" ]; then
  mv "$BINARY_PATH" "${INSTALL_DIR}/open-actions"
else
  echo "Error: ${INSTALL_DIR} is not writable" >&2
  echo "Set INSTALL_DIR to a writable path and try again" >&2
  exit 1
fi

echo "open-actions installed to ${INSTALL_DIR}/open-actions"

if ! echo "$PATH" | tr ':' '\n' | grep -Fqx "$INSTALL_DIR"; then
  echo ""
  echo "Add open-actions to your PATH by adding the following to your shell profile:"
  echo "  export PATH=\"${INSTALL_DIR}:\$PATH\""
fi
