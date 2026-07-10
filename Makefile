# cordn-rs — release helper.
#
#   make patch | minor | major
#
# Bumps the single workspace version in Cargo.toml, refreshes Cargo.lock,
# commits, tags vX.Y.Z, and pushes the tag — which triggers
# .github/workflows/release.yml to build and publish binaries.

VERSION := $(shell sed -nE 's/^version = "([0-9.]+)"/\1/p' Cargo.toml | head -1)

.PHONY: show verify-clean patch minor major

show:
	@echo "$(VERSION)"

verify-clean:
	@if [ -n "$$(git status --porcelain)" ]; then \
		echo "working tree not clean; commit or stash first" >&2; exit 1; \
	fi

patch minor major: verify-clean
	@set -e; \
	new=$$(./scripts/bump-version.sh "$(VERSION)" "$@"); \
	echo "release $(VERSION) → $$new"; \
	sed -i.bak -E 's/^(version = ")[0-9.]+/\1'"$$new"'/' Cargo.toml && rm -f Cargo.toml.bak; \
	cargo check --quiet; \
	git add Cargo.toml Cargo.lock; \
	git commit -q -m "release v$$new"; \
	git tag "v$$new"; \
	git push origin "v$$new" HEAD; \
	echo "✓ pushed v$$new — release workflow triggered"
