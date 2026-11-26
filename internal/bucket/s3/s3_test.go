package s3_test

import (
	"context"
	"errors"
	"os"
	"testing"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/majorcontext/motte/internal/bucket"
	"github.com/majorcontext/motte/internal/bucket/buckettest"
	"github.com/majorcontext/motte/internal/bucket/s3"
)

const (
	endpointEnv = "MOTTE_TEST_S3_ENDPOINT"
	bucketEnv   = "MOTTE_TEST_S3_BUCKET"
)

func TestConformance(t *testing.T) {
	t.Parallel()
	endpoint := os.Getenv(endpointEnv)
	if endpoint == "" {
		t.Skip(endpointEnv + " is not set: the test needs an S3-compatible endpoint such as MinIO, and AWS_* credentials")
	}
	name := os.Getenv(bucketEnv)
	if name == "" {
		name = "motte-test"
	}
	ctx := context.Background()
	client, err := s3.NewClient(ctx, s3.Options{Endpoint: endpoint, Region: "us-east-1", PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: &name})
	var owned *types.BucketAlreadyOwnedByYou
	if err != nil && !errors.As(err, &owned) {
		t.Fatal(err)
	}
	buckettest.Run(t, func(*testing.T) bucket.Store { return s3.New(client, name) }, buckettest.Caps{})
}
