package main

import (
	"os"
	"testing"

	"github.com/majorcontext/motte/internal/testcount"
)

func TestMain(m *testing.M) { os.Exit(testcount.Run(m)) }
