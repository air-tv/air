#!/usr/bin/env bash
set -euo pipefail

repository_path="${1:-}"
expected_manifest_hash="${2:-}"
if [[ -z "$repository_path" || ! "$expected_manifest_hash" =~ ^[0-9a-f]{64}$ ]]; then
  echo "Usage: $0 MAVEN_REPOSITORY EXPECTED_SHA256SUMS_HASH" >&2
  exit 1
fi

repository_path="$(cd "$repository_path" && pwd -P)"
manifest="$repository_path/SHA256SUMS"
test -f "$manifest"

actual_manifest_hash="$(sha256sum "$manifest" | awk '{print $1}')"
if [[ "$actual_manifest_hash" != "$expected_manifest_hash" ]]; then
  echo "SHA256SUMS transport hash mismatch" >&2
  exit 1
fi

actual_paths="$(mktemp)"
manifest_paths="$(mktemp)"
trap 'rm -f "$actual_paths" "$manifest_paths"' EXIT
(
  cd "$repository_path"
  find . -type f ! -path './SHA256SUMS' -print | LC_ALL=C sort > "$actual_paths"
)
if ! sed -nE 's/^[0-9a-f]{64}  (.+)$/\1/p' "$manifest" | LC_ALL=C sort > "$manifest_paths"; then
  exit 1
fi
test "$(wc -l < "$manifest")" -eq "$(wc -l < "$manifest_paths")"
cmp -s "$actual_paths" "$manifest_paths"

(
  cd "$repository_path"
  sha256sum --strict -c SHA256SUMS
)

rm "$manifest"
