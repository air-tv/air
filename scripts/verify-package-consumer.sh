#!/usr/bin/env bash
set -euo pipefail

repository_path="${1:-}"
if [[ -z "$repository_path" ]]; then
  echo "Usage: $0 MERGED_MAVEN_REPOSITORY" >&2
  exit 1
fi

repository_path="$(realpath -m "$repository_path")"
test -d "$repository_path"
repository_uri="file://$repository_path"
script_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
air_directory="$(cd "$script_directory/.." && pwd)"

for coordinate in \
  stremio-addon-client/0.1.0/stremio-addon-client-0.1.0.module \
  iptv/0.1.0/iptv-0.1.0.module \
  video/0.2.0/video-0.2.0.module \
  stremio-addon-client-mingwx64/0.1.0/stremio-addon-client-mingwx64-0.1.0.klib \
  iptv-iosarm64/0.1.0/iptv-iosarm64-0.1.0.klib \
  video-macosarm64/0.2.0/video-macosarm64-0.2.0.klib
do
  test -f "$repository_path/com/getair/$coordinate"
done

sqlite_library="$(cc -print-file-name=libsqlite3.so)"
test -f "$sqlite_library"

cd "$air_directory"
AIR_SQLITE_LIBRARY_DIR="$(dirname "$sqlite_library")" ./gradlew \
  assertPackageDependencyMode \
  compileCommonMainKotlinMetadata \
  compileKotlinJvm \
  compileKotlinJs \
  compileKotlinWasmJs \
  compileReleaseKotlinAndroid \
  compileKotlinLinuxX64 \
  -PuseLocalAirBuilds=false \
  -PgetAirPackageRepository="$repository_uri" \
  --refresh-dependencies \
  --max-workers=2 \
  --no-daemon \
  --no-configuration-cache
