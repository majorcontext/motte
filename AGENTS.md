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
- Run Go tests with `-race`.

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

```bash
go build ./...
go vet ./...
go test -race ./...
test -z "$(gofmt -l .)"
LEAK_DENYLIST="$(cat denylist.txt)" go run ./internal/leaklint
```
