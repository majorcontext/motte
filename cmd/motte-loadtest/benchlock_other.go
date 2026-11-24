//go:build !unix

package main

import "errors"

func lockHost(string) (func(), error) { return nil, errors.New("bench: needs a unix host") }
