#!/usr/bin/env bash
set -euo pipefail

arch="$1"
manifest="$2"
layer="$3"

fail() {
    echo "image_binary_test: $*" >&2
    exit 1
}

case "${arch}" in
    amd64) want_machine="3e00" ;;
    arm64) want_machine="b700" ;;
    *) fail "unsupported architecture ${arch}" ;;
esac

work="${TEST_TMPDIR:-$(mktemp -d)}"
rootfs="${work}/rootfs"
mkdir -p "${rootfs}"
tar -xf "${layer}" -C "${rootfs}"

binary="${rootfs}/usr/bin/krabka"
[[ -x "${binary}" ]] || fail "krabka is not executable"
[[ "$(od -An -tx1 -N 4 "${binary}" | tr -d ' \n')" == "7f454c46" ]] || fail "krabka is not ELF"
machine="$(od -An -tx1 -j 18 -N 2 "${binary}" | tr -d ' \n')"
[[ "${machine}" == "${want_machine}" ]] || fail "binary architecture ${machine} does not match ${arch}"

if command -v sha256sum >/dev/null 2>&1; then
    digest="$(sha256sum "${layer}" | cut -d' ' -f1)"
else
    digest="$(shasum -a 256 "${layer}" | cut -d' ' -f1)"
fi
grep -qF "sha256:${digest}" "${manifest}" || fail "image does not reference its application layer"

host_arch="$(uname -m)"
native=false
case "${arch}:${host_arch}" in
    amd64:x86_64 | amd64:amd64 | arm64:aarch64 | arm64:arm64) native=true ;;
esac

if [[ "$(uname -s)" == "Linux" && "${native}" == true ]]; then
    "${binary}" gres --help >/dev/null
fi
echo "image_binary_test: krabka is a ${arch} ELF with the gres subcommand"
