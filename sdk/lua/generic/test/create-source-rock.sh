#!/bin/sh
set -eu

package_root=${DILOCK_LUA_PACKAGE_ROOT:-/src}
output=${1:-"$PWD/dilock-0.1.0-1.src.rock"}
temp_root=$(mktemp -d)
trap 'rm -rf "$temp_root"' EXIT HUP INT TERM
archive_root="$temp_root/dilock-0.1.0-1"
mkdir -p "$archive_root/lib"
cp "$package_root/lib/dilock.lua" "$archive_root/lib/dilock.lua"
cp "$package_root/LICENSE" "$archive_root/LICENSE"
cp "$package_root/README.md" "$archive_root/README.md"
tar -C "$temp_root" -czf "$temp_root/dilock-0.1.0-1.tar.gz" dilock-0.1.0-1

# LuaRocks pack needs an archive URL. Use an ephemeral local file URL so the
# source rock can be built and consumed offline without a fabricated registry
# or upstream URL. `luarocks make` continues to use the checkout directly.
awk -v archive="file://$temp_root/dilock-0.1.0-1.tar.gz" '
  /^source = \{/ {
    print "source = {"
    printf "   url = \"%s\",\n", archive
    print "   dir = \"dilock-0.1.0-1\","
    skip_source = 1
    next
  }
  skip_source && /^}/ { print "}"; skip_source = 0; next }
  skip_source { next }
  { print }
' "$package_root/dilock-0.1.0-1.rockspec" > "$temp_root/dilock-0.1.0-1.rockspec"

output_dir=$(dirname "$output")
output_name=$(basename "$output")
mkdir -p "$output_dir"
(
  cd "$output_dir"
  luarocks --lua-version=5.1 pack "$temp_root/dilock-0.1.0-1.rockspec"
)
generated="$output_dir/dilock-0.1.0-1.src.rock"
if [ "$generated" != "$output" ]; then
  mv "$generated" "$output"
fi
printf 'Created local source rock: %s\n' "$output"
