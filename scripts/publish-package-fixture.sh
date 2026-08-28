#!/usr/bin/env bash
set -euo pipefail

host="${1:-}"
repository_path="${2:-}"
if [[ -z "$repository_path" ]]; then
  echo "Usage: $0 linux|apple|windows ABSOLUTE_TEMP_MAVEN_REPOSITORY" >&2
  exit 1
fi

case "$host" in
  linux)
    publication_tasks=(
      publishAndroidReleasePublicationToMavenLocal
      publishJvmPublicationToMavenLocal
      publishJsPublicationToMavenLocal
      publishWasmJsPublicationToMavenLocal
      publishLinuxX64PublicationToMavenLocal
      publishKotlinMultiplatformPublicationToMavenLocal
    )
    ;;
  apple)
    publication_tasks=(
      publishMacosX64PublicationToMavenLocal
      publishMacosArm64PublicationToMavenLocal
      publishIosX64PublicationToMavenLocal
      publishIosArm64PublicationToMavenLocal
      publishIosSimulatorArm64PublicationToMavenLocal
    )
    ;;
  windows)
    publication_tasks=(publishMingwX64PublicationToMavenLocal)
    ;;
  *) echo "Unknown package fixture host: $host" >&2; exit 1 ;;
esac

if command -v cygpath >/dev/null 2>&1; then
  repository_path="$(cygpath -u "$repository_path")"
fi
if [[ -e "$repository_path" ]]; then
  test -d "$repository_path"
  test -z "$(find "$repository_path" -mindepth 1 -print -quit)"
else
  mkdir -p "$repository_path"
fi
repository_path="$(cd "$repository_path" && pwd -P)"

script_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
air_directory="$(cd "$script_directory/.." && pwd)"
workspace_directory="$(cd "$air_directory/.." && pwd)"

publish_contract() {
  local project="$1"
  local version="$2"
  local project_directory="$workspace_directory/$project"
  test -f "$project_directory/settings.gradle.kts"
  (
    cd "$project_directory"
    ./gradlew \
      "${publication_tasks[@]}" \
      -PVERSION_NAME="$version" \
      -Dmaven.repo.local="$repository_path" \
      --max-workers=2 \
      --no-daemon \
      --no-configuration-cache
  )
}

publish_contract stremio-addon-client 0.1.0
publish_contract iptv 0.1.0
publish_contract video 0.2.0

test -n "$(find "$repository_path/com/getair" -type f -name '*.pom' -print -quit)"
