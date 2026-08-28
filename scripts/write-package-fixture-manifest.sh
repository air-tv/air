#!/usr/bin/env bash
set -euo pipefail

repository_path="${1:-}"
if [[ -z "$repository_path" ]]; then
  echo "Usage: $0 MAVEN_REPOSITORY" >&2
  exit 1
fi

if command -v cygpath >/dev/null 2>&1; then
  repository_path="$(cygpath -u "$repository_path")"
fi
repository_path="$(cd "$repository_path" && pwd -P)"
manifest="$repository_path/SHA256SUMS"
temporary_manifest="$repository_path/.SHA256SUMS.tmp"

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

rm -f "$temporary_manifest"
: > "$temporary_manifest"
while IFS= read -r relative; do
  digest="$(sha256_of "$repository_path/${relative#./}")"
  printf '%s  %s\n' "$digest" "$relative" >> "$temporary_manifest"
done < <(
  cd "$repository_path"
  find . -type f ! -path './SHA256SUMS' ! -path './.SHA256SUMS.tmp' -print | LC_ALL=C sort
)

test -s "$temporary_manifest"
mv "$temporary_manifest" "$manifest"
sha256_of "$manifest"
