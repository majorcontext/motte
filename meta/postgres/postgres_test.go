package postgres_test

import (
	"testing"

	"github.com/majorcontext/motte/internal/pgtest"
	"github.com/majorcontext/motte/meta"
	"github.com/majorcontext/motte/meta/metatest"
	"github.com/majorcontext/motte/meta/postgres"
)

func TestConformance(t *testing.T) {
	metatest.Run(t, func(t *testing.T) meta.Store { return &postgres.Store{Pool: pgtest.Pool(t, "")} })
}
