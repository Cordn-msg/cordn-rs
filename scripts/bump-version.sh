#!/usr/bin/env bash
# bump-version.sh <current> <patch|minor|major>  →  prints the next semver.
# Pure bash, no deps. Used by the Makefile release targets.
set -euo pipefail

current="${1:?usage: bump-version.sh <current> <patch|minor|major>}"
kind="${2:?usage: bump-version.sh <current> <patch|minor|major>}"

IFS='.' read -r major minor patch <<<"$current"
case "$kind" in
	patch) patch=$((patch + 1)) ;;
	minor) minor=$((minor + 1)); patch=0 ;;
	major) major=$((major + 1)); minor=0; patch=0 ;;
	*) echo "bad kind '$kind' (want patch|minor|major)" >&2; exit 1 ;;
esac

printf '%s.%s.%s\n' "$major" "$minor" "$patch"
