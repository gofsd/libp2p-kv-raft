#!/bin/sh
# Fails a PR that touches e2e-relevant protocol/daemon/client code without
# also touching test/e2e/testdata.json -- the CI-side backstop for the
# pre-push hook's `mage e2e:current` gate (scripts/git-hooks/pre-push),
# which only runs locally and only if a developer ran
# `mage githooks:install`. This check can't replay real e2e itself (no
# SSH-reachable bootstrap host or Android emulator on a generic hosted
# runner -- see CONTRIBUTING.md's "Why full e2e isn't in CI"); it only
# catches the specific failure mode of "e2e-relevant code changed and
# testdata.json didn't," which is exactly what a skipped/uninstalled hook
# would let through silently.
#
# Escape hatch: include "SKIP_E2E_CHECK" anywhere in the PR title or the
# HEAD commit message (mirrors the pre-push hook's own SKIP_E2E=1 --
# deliberate and visible, not a silent bypass) for a change that genuinely
# doesn't need a new e2e row (e.g. a change to the e2e harness/tooling
# itself, or docs-only).
#
# Usage: check-e2e-gate.sh <base_sha> <head_sha> <pr_title>

set -eu

base_sha="$1"
head_sha="$2"
pr_title="${3:-}"

if printf '%s' "$pr_title" | grep -q "SKIP_E2E_CHECK"; then
	echo "check-e2e-gate: SKIP_E2E_CHECK in PR title, skipping"
	exit 0
fi
if git log -1 --format=%B "$head_sha" | grep -q "SKIP_E2E_CHECK"; then
	echo "check-e2e-gate: SKIP_E2E_CHECK in HEAD commit message, skipping"
	exit 0
fi

changed=$(git diff --name-only "$base_sha" "$head_sha")

# Paths whose behavior e2e rows actually exercise: the wire protocol, the
# daemon and its transport/relay/join logic, and every client bridge a
# testdata.json row can dispatch through (desktop, android, web). Deliberately
# excludes pkg/e2erun/pkg/e2edata (the harness itself) and mage/CI/docs --
# changing the harness doesn't inherently require a new recorded row.
e2e_relevant=$(printf '%s\n' "$changed" | grep -E '^(api/shmevent\.capnp|pkg/shmevent/|pkg/daemon/|pkg/kvfsm/|pkg/store/|pkg/raft/|pkg/ipc/|pkg/shmclient/|pkg/kvctl/|cmd/kvnode/|cmd/kvctl-cli/|mobile/kvmobile/|web-app/src/)' || true)

if [ -z "$e2e_relevant" ]; then
	echo "check-e2e-gate: no e2e-relevant paths changed, nothing to check"
	exit 0
fi

if printf '%s\n' "$changed" | grep -q '^test/e2e/testdata\.json$'; then
	echo "check-e2e-gate: test/e2e/testdata.json updated alongside e2e-relevant changes, ok"
	exit 0
fi

echo "check-e2e-gate: FAILED" >&2
echo "" >&2
echo "This PR changes e2e-relevant path(s):" >&2
printf '%s\n' "$e2e_relevant" | sed 's/^/  /' >&2
echo "" >&2
cat >&2 <<'EOF'
...but does not touch test/e2e/testdata.json. This project's real e2e
coverage only runs locally (mage e2e:current via the pre-push hook -- see
CONTRIBUTING.md) and is easy to skip by forgetting to run
`mage githooks:install`, or by pushing with SKIP_E2E=1. This check is the
CI-side backstop for that: either add e2e row(s) covering this change
(mage e2e:addtest, then mage e2e:current locally to confirm and record it),
or, if this change genuinely doesn't need one, add "SKIP_E2E_CHECK" to the
PR title or the HEAD commit message.
EOF
exit 1
