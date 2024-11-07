#!/bin/bash

set -euo pipefail

RESOURCES=$(dirname $0)/../src/main/resources/aircompressor

download_linux()
{
    echo "Download $3 ..."
    OUT="$RESOURCES/$3"
    if [ -f "$OUT" ]; then
        echo "=> skipped"
        return
    fi

    TEMP=$(mktemp)
    curl -sSL "$1" | tar -xO data.tar.xz | tar -xO ".$2" > "$TEMP"
    mv -f "$TEMP" "$OUT"
    chmod 644 "$OUT"
    echo "=> downloaded"
}

download_macos()
{
    echo "Download $5 ..."
    OUT="$RESOURCES/$5"
    if [ -f "$OUT" ]; then
        echo "=> skipped"
        return
    fi

    DIGEST=$(curl -sS -L \
        -H 'Authorization: Bearer QQ==' \
        -H 'Accept: application/vnd.oci.image.index.v1+json' \
        "https://ghcr.io/v2/homebrew/core/$1/manifests/$2"  \
        | jq -r "
            .manifests |
            sort_by(.platform[\"os.version\"]) |
            .[] |
            select(.platform.os == \"darwin\") |
            select(.platform.architecture == \"$4\") |
            .annotations[\"sh.brew.bottle.digest\"]" \
        | head -n1)

    TEMP=$(mktemp)
    curl -sS -L \
        -H 'Authorization: Bearer QQ==' \
        "https://ghcr.io/v2/homebrew/core/$1/blobs/sha256:$DIGEST" | \
        tar -xO "$1/$2/lib/$3" > "$TEMP"
    mv -f "$TEMP" "$OUT"
    chmod 644 "$OUT"
    echo "=> downloaded"
}

# Snappy
download_linux \
  "https://deb.debian.org/debian/pool/main/s/snappy/libsnappy1v5_1.2.1-1+b1_amd64.deb" \
  "/usr/lib/x86_64-linux-gnu/libsnappy.so.1.2.1" \
  "linux-amd64/libsnappy.so"

download_linux \
  "https://deb.debian.org/debian/pool/main/s/snappy/libsnappy1v5_1.2.1-1+b1_arm64.deb" \
  "/usr/lib/aarch64-linux-gnu/libsnappy.so.1.2.1" \
  "linux-aarch64/libsnappy.so"

download_linux \
  "https://deb.debian.org/debian/pool/main/s/snappy/libsnappy1v5_1.2.1-1+b1_ppc64el.deb" \
  "/usr/lib/powerpc64le-linux-gnu/libsnappy.so.1.2.1" \
  "linux-ppc64le/libsnappy.so"

download_macos \
  snappy 1.1.10 libsnappy.1.1.10.dylib amd64 macos-amd64/libsnappy.dylib

download_macos \
  snappy 1.1.10 libsnappy.1.1.10.dylib arm64 macos-aarch64/libsnappy.dylib

# Zstandard
download_linux \
  "https://deb.debian.org/debian/pool/main/libz/libzstd/libzstd1_1.5.6+dfsg-1_amd64.deb" \
  "/usr/lib/x86_64-linux-gnu/libzstd.so.1.5.6" \
  "linux-amd64/libzstd.so"

download_linux \
  "https://deb.debian.org/debian/pool/main/libz/libzstd/libzstd1_1.5.6+dfsg-1_arm64.deb" \
  "/usr/lib/aarch64-linux-gnu/libzstd.so.1.5.6" \
  "linux-aarch64/libzstd.so"

download_linux \
  "https://deb.debian.org/debian/pool/main/libz/libzstd/libzstd1_1.5.6+dfsg-1_ppc64el.deb" \
  "/usr/lib/powerpc64le-linux-gnu/libzstd.so.1.5.6" \
  "linux-ppc64le/libzstd.so"

download_macos \
  zstd 1.5.6 libzstd.1.5.6.dylib amd64 macos-amd64/libzstd.dylib

download_macos \
  zstd 1.5.6 libzstd.1.5.6.dylib arm64 macos-aarch64/libzstd.dylib

# LZ4
download_linux \
  "https://deb.debian.org/debian/pool/main/l/lz4/liblz4-1_1.10.0-1_amd64.deb" \
  "/usr/lib/x86_64-linux-gnu/liblz4.so.1.10.0" \
  "linux-amd64/liblz4.so"

download_linux \
  "https://deb.debian.org/debian/pool/main/l/lz4/liblz4-1_1.10.0-1_arm64.deb" \
  "/usr/lib/aarch64-linux-gnu/liblz4.so.1.10.0" \
  "linux-aarch64/liblz4.so"

download_linux \
  "https://deb.debian.org/debian/pool/main/l/lz4/liblz4-1_1.10.0-1_ppc64el.deb" \
  "/usr/lib/powerpc64le-linux-gnu/liblz4.so.1.10.0" \
  "linux-ppc64le/liblz4.so"

download_macos \
  lz4 1.10.0 liblz4.1.10.0.dylib amd64 macos-amd64/liblz4.dylib

download_macos \
  lz4 1.10.0 liblz4.1.10.0.dylib arm64 macos-aarch64/liblz4.dylib

# bzip2
download_linux \
  "https://deb.debian.org/debian/pool/main/b/bzip2/libbz2-1.0_1.0.8-6_amd64.deb" \
  "/usr/lib/x86_64-linux-gnu/libbz2.so.1.0.4" \
  "linux-amd64/libbz2.so"

download_linux \
  "https://deb.debian.org/debian/pool/main/b/bzip2/libbz2-1.0_1.0.8-6_arm64.deb" \
  "/usr/lib/aarch64-linux-gnu/libbz2.so.1.0.4" \
  "linux-aarch64/libbz2.so"

download_linux \
  "https://deb.debian.org/debian/pool/main/b/bzip2/libbz2-1.0_1.0.8-6_ppc64el.deb" \
  "/usr/lib/powerpc64le-linux-gnu/libbz2.so.1.0.4" \
  "linux-ppc64le/libbz2.so"
