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
  stremio-addon-client/0.0.0-ci/stremio-addon-client-0.0.0-ci.module \
  iptv/0.0.0-ci/iptv-0.0.0-ci.module \
  video/0.0.0-ci/video-0.0.0-ci.module \
  stremio-addon-client-mingwx64/0.0.0-ci/stremio-addon-client-mingwx64-0.0.0-ci.klib \
  iptv-iosarm64/0.0.0-ci/iptv-iosarm64-0.0.0-ci.klib \
  video-macosarm64/0.0.0-ci/video-macosarm64-0.0.0-ci.klib
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
  -PgetAirStremioVersion=0.0.0-ci \
  -PgetAirIptvVersion=0.0.0-ci \
  -PgetAirVideoVersion=0.0.0-ci \
  --refresh-dependencies \
  --max-workers=2 \
  --no-daemon \
  --no-configuration-cache
