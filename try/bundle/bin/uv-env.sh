# Sourced by bin/medios-try and bin/medios-python. Makes sure a pinned uv is
# present under the state directory and exports the paths both scripts use.
# Everything it writes stays under MEDIOS_TRY_HOME, which is <bundle>/state
# unless set.

bundle=$(cd "$(dirname "$0")/.." && pwd)
state=${MEDIOS_TRY_HOME:-$bundle/state}
tools=$state/tools
mkdir -p "$tools"

# Parameter: uv version and archive checksums, pinned per bundle revision.
UV_VERSION=0.13.7
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64)   uv_target=aarch64-apple-darwin
                  uv_sha=81b811b57e4624df7ed3aac148e0359100d9962f3d9dce00aec533dc2e086731 ;;
  Linux-x86_64)   uv_target=x86_64-unknown-linux-gnu
                  uv_sha=c5221ff41b7892bfc223543883600125f6a47fe845093e0f5bdff4f359196d83 ;;
  Linux-aarch64)  uv_target=aarch64-unknown-linux-gnu
                  uv_sha=71279f8d3c67241e807b9d8db872d1bf541579fb9f08b7d1eb1682196d1e57dc ;;
  *) printf '{"ok":false,"step":"platform","error":"unsupported platform %s-%s; supported: Darwin-arm64, Linux-x86_64, Linux-aarch64"}\n' "$(uname -s)" "$(uname -m)"
     exit 2 ;;
esac

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

uv=$tools/uv-$UV_VERSION/uv
if [ ! -x "$uv" ]; then
  archive=$tools/uv-$uv_target.tar.gz
  curl -fsSL --retry 3 -o "$archive" \
    "https://github.com/astral-sh/uv/releases/download/$UV_VERSION/uv-$uv_target.tar.gz" || {
    printf '{"ok":false,"step":"uv-download","error":"could not download uv %s"}\n' "$UV_VERSION"; exit 1; }
  got=$(sha256 "$archive")
  if [ "$got" != "$uv_sha" ]; then
    printf '{"ok":false,"step":"uv-verify","error":"uv archive sha256 %s does not match pinned %s"}\n' "$got" "$uv_sha"
    rm -f "$archive"; exit 1
  fi
  mkdir -p "$tools/uv-$UV_VERSION"
  tar -xzf "$archive" -C "$tools/uv-$UV_VERSION" --strip-components 1
  rm -f "$archive"
fi

# Parameter: Python version for medios-try and for frameworks the agent writes.
MEDIOS_TRY_PYTHON=3.12
export MEDIOS_TRY_HOME=$state MEDIOS_TRY_BUNDLE=$bundle
export UV_CACHE_DIR=$state/uv-cache UV_PYTHON_INSTALL_DIR=$state/python UV_NO_CONFIG=1
