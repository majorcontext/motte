// Command lint runs the repository analyzers. Use: go run ./internal/lint/cmd/lint ./...
package main

import (
	"golang.org/x/tools/go/analysis/multichecker"

	"github.com/majorcontext/motte/internal/lint/paralleltests"
	"github.com/majorcontext/motte/internal/lint/wallclock"
)

func main() { multichecker.Main(wallclock.Analyzer, paralleltests.Analyzer) }
