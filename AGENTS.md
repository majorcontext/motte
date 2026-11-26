# AGENTS.md

Rules for AI coding agents in this repository.

## Read first

Read `docs/architecture.md` before you change behavior. It is the contract. A difference from it is a defect. A spec change updates the spec in the same pull request.

## Public repository

This repository is public. No file, commit message, branch name, pull request title or body, or comment may name an internal project, host, address, bucket, person or product of a caller. Use "workspace" for the identity concept and "the caller" or "a control plane" for the client.

The CI job `leak` runs `internal/leaklint` against the secret `LEAK_DENYLIST`. Never commit denylist content.

## Tests

- Write the failing contract test first for a change in what a caller receives.
- Write a unit test only for pure code: codecs, the grant, the pack format.
- Name the failure a test catches before you write it.
- Added test lines stay at or below added code lines.
- No wall-clock waits in tests: `time.Sleep` and `time.After` only inside a `synctest.Test` bubble; otherwise use channels. A wait that no bubble can hold, such as a cross-process wait, carries a `//lint:allow wallclock REASON` comment on its line. The analyzer `internal/lint/wallclock` enforces this.
- Run Go tests with `-race`.
- A test is at most 10 s and a package at most 10 s, unless `testdata/test-time-budget.txt` says more. Make a slow test fast; raise a budget only with a stated reason in the same change. `internal/testtime` enforces the budget in CI. CI times each package alone (`go test -p 1`), so a package time is its own cost.

### Test cost

Test time differs by machine, so the repository limits the causes of slow tests exactly and keeps time only as a calibrated backstop.

- Every top-level `TestXxx` calls `t.Parallel()` on its own `t`, directly or through a helper of the same package that calls it. A test that cannot run in parallel is listed in `testdata/serial-tests.txt`, one `package TestName reason` per line, where `package` is the import path below the module path. A line without a reason, and a line that names a test that does not exist or already runs in parallel, fails the analyzer `internal/lint/paralleltests`.
- PostgreSQL tests use `MOTTE_TEST_DATABASE_URL` when it is set. Otherwise `pgtest` starts one throwaway cluster for each package from `initdb` and `postgres` on `PATH` or in `MOTTE_TEST_PG_BIN`, and the cluster counts as one process. `MOTTE_TEST_REQUIRE_PG=1` makes a missing database fail the test instead of skipping it.
- A test helper that starts a server (`testserver.Start`, `testserver.StartLocal`), creates a PostgreSQL schema (`pgtest.Pool`) or starts a process (`testcount.Command`) counts it through `internal/testcount`. A package that uses these helpers calls `testcount.Run` from its `TestMain`; the helpers fail a test in a package that does not. At the end of the run the package prints one line `motte-testcount {"processes":N,"schemas":N,"servers":N}`. Under `-count=N` the counts are those of one run. A cached run prints the same line.
- `testdata/test-time-budget.txt` holds one limit for each count: `count PATH NAME MAX` for one package, `count default NAME MAX` for a package without its own line. A package or name without a line, and without a default, has the limit 0. `internal/testtime` fails a package whose count passes its limit and prints the counts. A change that raises a count raises its line in the same diff and states why.
- `testtime` runs a fixed calibration workload (a CPU loop and a loop of file create, write, stat and remove) after the test stream ends, when the machine is idle. The line `calibrate SECONDS` of the budget file is the calibration time on the reference runner, the GitHub `ubuntu-latest` job. `testtime` multiplies every time budget by measured divided by reference, and never by less than 1. It prints the calibration time and the factor.
- `testtime` prints, for each package, the sum of the time of its leaf tests divided by the wall time of the package. The ratio has no limit.

## Provider seams

- Put each external provider behind an interface that the consumer owns. See `docs/architecture.md` "Provider seams".
- Give each seam a conformance suite that every implementation runs, and at least two implementations. A local or in-memory implementation counts.
- Import a provider SDK only in an implementation package or under `cmd/`.
- Name a provider host only in a file of `testdata/provider-hosts.txt`.
- Update the seams table in the same diff as a change to a seam.
- A seam that does not meet the rule is a gap row in the table and a line of `testdata/seam-gaps.txt`. The list only shrinks.
- `internal/seams` enforces these rules in `go test`.

## Comments

Write no comment by default. A comment states why: a constraint, a hazard, or a rejected alternative. It never restates the code. It carries no history, no date, no person, no pull request number and no path to justify the code.

## Style

- Use standard Go style. Run `gofmt` and `go vet`.
- Use ASD-STE100 Simplified Technical English for prose. Use one term for one concept and the active voice.
- Write tooling in Go.

## Commits and pull requests

- Use Conventional Commit subjects: `type(scope): description`, lowercase, no final period.
- Squash merge.
- A commit body gives the problem, the design, the semantic change and the verification. Hard-wrap it at 76 columns.
- Do not hard-wrap a pull request body.
- Add no attribution footer.

## Checks

ward lands a branch onto `main` only after it runs the checks of `.ward/checks.json`; the file mirrors the commands below in two checks, because the runner runs the checks of a repository one after another: `static` (build, vet, lint, gofmt) and `budget`, which runs every test with `-race` and so covers a plain `go test`. Its check `budget` runs the `testtime` command with `-time-allowance 1.15`, because the land runners are slower than the CI reference: the counts of processes, servers and schemas stay strict, and the time limits grow by that factor beside the calibration factor. CI keeps the strict limits.

```bash
go build ./...
go vet ./...
go run ./internal/lint/cmd/lint ./...
go test -race -count=1 -p 1 -json ./... | go run ./internal/testtime
test -z "$(gofmt -l .)"
LEAK_DENYLIST="$(cat denylist.txt)" go run ./internal/leaklint
```
