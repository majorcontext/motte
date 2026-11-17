package store_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/vfs/store"
	"github.com/majorcontext/motte/testserver"
)

// TestHTTPStoreSpeaksTheStoreWire drives HTTPStore against the real server, so a wire change in either breaks here.
func TestHTTPStoreSpeaksTheStoreWire(t *testing.T) {
	ctx := context.Background()
	srv := testserver.Start(t, "")
	const repo = "gh:acme/widgets"
	cl := client.New(srv.URL, client.StaticGrant(srv.OperatorGrant(t)))
	if _, err := cl.Claim(ctx, "w1", api.ClaimRequest{Epoch: 1, Owner: "o", Repo: repo}); err != nil {
		t.Fatal(err)
	}
	tok := srv.NodeGrant(t, "w1", 1, repo)
	st := &store.HTTPStore{BaseURL: srv.URL, Grant: func() (string, error) { return tok, nil }}

	blob := gitpack.Object{Type: gitpack.Blob, Data: []byte("hello\n")}
	blobSHA := gitpack.Hash(gitpack.Blob, blob.Data)
	treeData := append([]byte("100644 a.txt\x00"), blobSHA[:]...)
	tree := gitpack.Object{Type: gitpack.Tree, Data: treeData}
	treeSHA := gitpack.Hash(gitpack.Tree, treeData)
	commit := gitpack.Object{Type: gitpack.Commit, Data: []byte(fmt.Sprintf("tree %s\nauthor a <a@x> 1700000000 +0000\ncommitter a <a@x> 1700000000 +0000\n\nm\n", treeSHA))}
	commitSHA := gitpack.Hash(gitpack.Commit, commit.Data)
	if err := st.PutObjects(ctx, []gitpack.Object{blob, tree, commit}); err != nil {
		t.Fatal(err)
	}
	gotTree, when, err := st.Commit(ctx, commitSHA)
	if err != nil || gotTree != treeSHA || when.Unix() != 1700000000 {
		t.Fatalf("Commit = %v %v %v", gotTree, when, err)
	}
	trees, err := st.TreeBulk(ctx, treeSHA, 100)
	if err != nil || len(trees) != 1 || len(trees[0].Entries) != 1 || trees[0].Entries[0].Name != "a.txt" || trees[0].Entries[0].SHA != blobSHA {
		t.Fatalf("TreeBulk = %+v %v", trees, err)
	}
	if blobs, err := st.Blobs(ctx, []gitpack.SHA{blobSHA}); err != nil || string(blobs[0]) != "hello\n" {
		t.Fatalf("Blobs = %q %v", blobs, err)
	}

	ref, err := st.SwapRef(ctx, "heads/main", 0, commitSHA.String())
	if err != nil || ref.Commit != commitSHA.String() {
		t.Fatalf("SwapRef create = %+v %v", ref, err)
	}
	var conflict *store.RefConflictError
	if _, err := st.SwapRef(ctx, "heads/main", ref.Generation+9, commitSHA.String()); !errors.As(err, &conflict) || conflict.Ref == nil || conflict.Ref.Generation != ref.Generation {
		t.Fatalf("stale SwapRef = %v, want a conflict that carries the stored ref", err)
	}

	if err := st.PutOp(ctx, "operations", "abababababababab", []byte("op")); err != nil {
		t.Fatal(err)
	}
	if got, err := st.GetOp(ctx, "operations", "abababababababab"); err != nil || string(got) != "op" {
		t.Fatalf("GetOp = %q %v", got, err)
	}
	if oh, err := st.SwapOpHeads(ctx, 0, []string{"abababababababab"}); err != nil || len(oh.Heads) != 1 || oh.Generation == 0 {
		t.Fatalf("SwapOpHeads = %+v %v", oh, err)
	}
	if err := st.PutWorkspace(ctx, "abababababababab", commitSHA.String(), 1); err != nil {
		t.Fatal(err)
	}
}
