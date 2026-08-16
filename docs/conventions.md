# Working conventions

Operational rules for this repo - the things that are easy to get wrong and
have already cost us time at least once. Everything here exists because
something went wrong without it; see `docs/issues.md` for the incidents.

## Two GitHub accounts, two independent settings

Track A work is committed and PR'd as `AfzalRaja001`. Track B work is
committed and PR'd as `Susan5504R` (display name `Sarah`).

**The git commit identity and the `gh` CLI's active account are separate
settings. Switching one does not switch the other.** Forgetting this put a
Track B PR under the wrong account once already.

Before starting Track A work:

```bash
git config user.name "AfzalRaja001" && git config user.email "183200737+AfzalRaja001@users.noreply.github.com" && gh auth switch -u AfzalRaja001
```

Before starting Track B work:

```bash
git config user.name "Sarah" && git config user.email "184747174+Susan5504R@users.noreply.github.com" && gh auth switch -u Susan5504R
```

To verify before committing:

```bash
git config user.name && git config user.email && gh auth status
```

The emails are GitHub noreply addresses, which link commits to the right
account without exposing a real address. The numeric prefix is account
specific - don't guess it. Recover it from history if unsure:

```bash
git log --all --format="%an <%ae>" | sort -u
```

## Branch from what you actually depend on

If new work needs code from an unmerged branch, branch from **that branch**,
not `main`. Branching from `main` in that situation silently drops the
dependency.

Always fetch first. A plain `git pull` can report "Already up to date" from
stale local refs even when the remote has moved:

```bash
git fetch origin && git checkout main && git merge --ff-only origin/main
```

If work turns out to be independent of an open PR (touching disjoint files),
prefer branching from `main` - it keeps the PR reviewable on its own and
avoids a merge-order dependency.

## Verification commands

Build and vet - both should be silent:

```bash
cd "C:\Users\Afzal\Desktop\kafka-go" && "C:\Program Files\Go\bin\go.exe" build ./... && "C:\Program Files\Go\bin\go.exe" vet ./...
```

Full test suite with the race detector. The `msys64` PATH addition is
required - `-race` needs cgo and therefore a C toolchain:

```bash
export PATH="$PATH:/c/msys64/ucrt64/bin" && "C:\Program Files\Go\bin\go.exe" test -race -count=1 ./...
```

`-count=1` disables the test cache. Use it whenever you need to be sure the
tests actually ran rather than replaying a cached pass.

## Don't trust a green test suite alone

Every phase gets verified against a **real running broker with a real Kafka
client**, not just unit tests. Four of the seven entries in `docs/issues.md`
were invisible to a fully-passing suite - including one silent data
corruption bug.

Start the broker:

```bash
cd "C:\Users\Afzal\Desktop\kafka-go" && "C:\Program Files\Go\bin\go.exe" build -o kafka-broker.exe ./cmd/broker && ./kafka-broker.exe
```

`kafka-python` client settings this broker requires (each one traced to a
missing feature - see `docs/issues.md` entry 3):

- `api_version=(2, 5, 0)` - forces magic-byte-2 record batches
- `enable_idempotence=False` - no `InitProducerId` support
- `linger_ms` / `batch_size` - raise these to force multi-record batching,
  which is the case that exposed the offset-span bug
- `assign()` + `seek()` rather than `subscribe()` - no consumer groups yet
  (Phase 4)

Wipe `data/` between runs when testing offset behaviour, so results aren't
confused by a previous run's log. `data/` is gitignored.

## When `go fmt` touches everything

`go fmt ./...` rewrites line endings across the whole tree because
`core.autocrlf` is `true` and there's no `.gitattributes`. After running it,
check `git diff --stat` and revert files whose only change is whitespace, so
the PR diff stays scoped to real changes.

## Small PRs, explained properly

`main` is protected: one approving review and a green `test` check are
required, and that applies to admins too. No direct pushes.

Every commit or PR gets a full lesson-depth write-up - see `CLAUDE.md` at the
repo root for what that means and why.
