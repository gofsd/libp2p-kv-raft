# Contributing

Thanks for considering a contribution. This project is a distributed
key-value store (hashicorp/raft over libp2p, pure-Go SQLite backend) with
desktop, Android, and browser/wasm clients sharing one wire protocol. Read
`README.md` first (kept current and detailed) and `CLAUDE.md` for a
condensed orientation.

## Setting up

- Go 1.25+ and [mage](https://magefile.org/) on `PATH` for everything
  desktop-side.
- Android: `gomobile`, an NDK (API 26+ required -- see README's "Follower
  on Android" section for why), and `adb` for `mage e2e`'s Android rows.
- Web: `wasm-pack`, Node/npm (`npm install` in `web-app/` once).

```bash
mage test          # unit tests: go test -v -short ./...
mage integration    # go test -v -tags=integration ./...
mage testall        # test + integration + every e2e:all row
```

Run a single package/test the normal Go way, e.g.
`go test ./pkg/daemon/... -run TestJoinThroughRelay -v`.

## Before opening a PR

- `go build ./...`, `go vet ./...`, and `gofmt -l` (no output) must be
  clean -- there's no `.golangci.yml`; these three are the baseline.
- `mage test`/`mage integration` must pass.
- If you touch anything in the shared wire protocol
  (`api/shmevent.capnp`, `pkg/shmevent`, `web-app/src/*.rs`), grep the
  Rust side and run its own tests too -- see CLAUDE.md's "Verify
  cross-language callers" note.
- Run `mage githooks:install` once so the pre-push hook
  (`mage e2e:current`) actually runs before every push -- see the next
  section for why this, not CI, is the full end-to-end gate.

## Why full e2e isn't in CI

`mage e2e:all`/`e2e:current` deploy and drive real processes: an
SSH-reachable bootstrap/relay leader, a real Android emulator, and a real
browser joining as a raft learner over WebTransport. None of that is
readily available on a generic hosted CI runner, so it stays a local,
pre-push gate (`mage githooks:install`) instead. CI (`.github/workflows/`)
covers what *is* portable: build/vet/lint, unit + integration tests, and
compile-only smoke checks for the Android/wasm targets (do they still
build, not "does a real cluster replicate"). If you're adding a
CI-friendly self-hosted runner with emulator/SSH access, that's a welcome
contribution on its own.

## Code style

- Comments explain *why*, not *what* -- see this repo's existing doc
  comments for the expected density and tone. Don't add a comment a
  well-named identifier already makes obvious.
- No premature abstraction: three similar lines beat a speculative helper
  built for a hypothetical second caller that doesn't exist yet.
- Match the surrounding file's error-handling/logging conventions rather
  than introducing a new pattern in one spot.

## Reporting a security issue

See `SECURITY.md` -- please don't open a public issue for a vulnerability.
