package testserver_test

import (
	"os"
	"testing"

	"github.com/majorcontext/motte/testserver"
)

func TestMain(m *testing.M) { os.Exit(testserver.Shared(m)) }
