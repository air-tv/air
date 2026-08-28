#!/usr/bin/env bash
set -euo pipefail

destination="${1:-}"
shift || true
if [[ -z "$destination" || "$#" -eq 0 ]]; then
  echo "Usage: $0 EMPTY_DESTINATION SOURCE..." >&2
  exit 1
fi

destination="$(realpath -m "$destination")"
if [[ -e "$destination" ]]; then
  test -d "$destination"
  test -z "$(find "$destination" -mindepth 1 -print -quit)"
else
  mkdir -p "$destination"
fi

for source in "$@"; do
  source="$(realpath "$source")"
  test -d "$source"
  while IFS= read -r -d '' file; do
    relative="${file#"$source"/}"
    target="$destination/$relative"
    if [[ -e "$target" || -L "$target" ]]; then
      echo "Package fixture collision: $relative" >&2
      exit 1
    fi
    mkdir -p "$(dirname "$target")"
    cp -p "$file" "$target"
  done < <(find "$source" -type f -print0)
done

test -n "$(find "$destination/com/getair" -type f -name '*.module' -print -quit)"
