package gcs_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"cloud.google.com/go/storage"

	"github.com/majorcontext/motte/bucket"
	"github.com/majorcontext/motte/bucket/buckettest"
	"github.com/majorcontext/motte/bucket/gcs"
)

const bucketEnv = "MOTTE_TEST_GCS_BUCKET"

func TestConformance(t *testing.T) {
	t.Parallel()
	name := os.Getenv(bucketEnv)
	if name == "" {
		t.Skip(bucketEnv + " is not set: the test needs a Cloud Storage bucket and credentials, or a fake server named by STORAGE_EMULATOR_HOST")
	}
	client, err := storage.NewClient(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if os.Getenv("STORAGE_EMULATOR_HOST") != "" {
		b := client.Bucket(name)
		_, err := b.Attrs(context.Background())
		if errors.Is(err, storage.ErrBucketNotExist) {
			err = b.Create(context.Background(), "motte-test", nil)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	buckettest.Run(t, func(*testing.T) bucket.Store { return gcs.New(client, name) }, buckettest.Caps{ConditionalDelete: true})
}
