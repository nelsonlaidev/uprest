#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <redis-js checkout> [junit report path]" >&2
  exit 2
fi

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
upstream_root="$(cd "$1" && pwd)"
exclusions_file="$repository_root/tests/compatibility/exclusions.txt"
patch_file="$repository_root/tests/compatibility/upstream.patch"
report_path="${2:-$upstream_root/compatibility-results.xml}"

if [[ ! -f "$upstream_root/packages/redis/package.json" ]]; then
  echo "redis-js checkout not found at $upstream_root" >&2
  exit 2
fi

while IFS= read -r path; do
  if [[ -z "$path" || "$path" == \#* ]]; then
    continue
  fi

  target="$upstream_root/$path"

  if [[ ! -f "$target" ]]; then
    echo "compatibility exclusion is missing upstream: $path" >&2
    exit 1
  fi

  rm "$target"
done < "$exclusions_file"

git -C "$upstream_root" apply --check --unidiff-zero "$patch_file"
git -C "$upstream_root" apply --unidiff-zero "$patch_file"

pnpm --dir "$upstream_root" install --frozen-lockfile

cd "$upstream_root/packages/redis"

bun test pkg \
  --timeout 20000 \
  --reporter=junit \
  --reporter-outfile="$report_path"
