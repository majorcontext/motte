package main

import (
	"os"
	"testing"

	"github.com/majorcontext/tools/testcount"
)

func TestMain(m *testing.M) { os.Exit(testcount.Run(m)) }
