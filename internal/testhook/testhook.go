// Package testhook gives the tests of this module the stores and keys behind a testserver.Server, which the public
// type does not expose, so that a test can run a server with a configuration that no option sets.
package testhook

import (
	"sync"

	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/bucket"
	"github.com/majorcontext/motte/meta"
)

// Stack is the state and the keys of one testserver.Server.
type Stack struct {
	Meta     meta.Store
	Bucket   bucket.Store
	Verifier *grant.Verifier
}

var stacks sync.Map

// Register records the stack of the server.
func Register(server any, s Stack) { stacks.Store(server, s) }

// Unregister forgets the stack of the server.
func Unregister(server any) { stacks.Delete(server) }

// Of returns the stack that Register recorded for the server.
func Of(server any) Stack {
	s, _ := stacks.Load(server)
	return s.(Stack)
}
