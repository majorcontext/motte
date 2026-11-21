// Package s3 backs the bucket seam with an S3-compatible object store: AWS
// S3, Cloudflare R2 or MinIO.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/majorcontext/motte/bucket"
	"github.com/majorcontext/motte/internal/clock"
)

const (
	maxCreateAttempts = 8
	backoffStart      = 20 * time.Millisecond
	backoffCap        = time.Second
)

type s3Bucket struct {
	c     *s3.Client
	name  string
	clock clock.Clock
}

// Options configures a client. Credentials come from the AWS SDK default
// chain. A zero Endpoint is the AWS endpoint of the region. A zero Region is
// the region of the default chain.
type Options struct {
	Endpoint  string
	Region    string
	PathStyle bool
}

// NewClient returns a client that sends checksums only when the API needs
// them: some S3-compatible services reject the default checksum headers.
func NewClient(ctx context.Context, o Options) (*s3.Client, error) {
	var load []func(*config.LoadOptions) error
	if o.Region != "" {
		load = append(load, config.WithRegion(o.Region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, load...)
	if err != nil {
		return nil, fmt.Errorf("bucket: s3 config: %w", err)
	}
	return s3.NewFromConfig(cfg, func(so *s3.Options) {
		if o.Endpoint != "" {
			so.BaseEndpoint = aws.String(o.Endpoint)
		}
		so.UsePathStyle = o.PathStyle
		so.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		so.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	}), nil
}

// New returns the Store of the bucket name.
func New(client *s3.Client, name string) bucket.Store { return s3Bucket{client, name, clock.Real()} }

func status(err error) int {
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

// Create retries a 409, which means a write of the key is in flight, with
// jittered exponential backoff, and fails after maxCreateAttempts answers.
func (b s3Bucket) Create(ctx context.Context, key string, data []byte) (bool, error) {
	delay := backoffStart
	for attempt := 1; ; attempt++ {
		_, err := b.c.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      &b.name,
			Key:         &key,
			Body:        bytes.NewReader(data),
			IfNoneMatch: new("*"),
		})
		switch status(err) {
		case 0:
			if err != nil {
				return false, fmt.Errorf("bucket: write: %w", err)
			}
			return true, nil
		case http.StatusPreconditionFailed:
			return false, nil
		case http.StatusConflict:
			if attempt == maxCreateAttempts {
				return false, fmt.Errorf("bucket: write: still in conflict after %d attempts: %w", attempt, err)
			}
			wait := delay/2 + rand.N(delay/2+1)
			if err := clock.Wait(ctx, b.clock, wait); err != nil {
				return false, fmt.Errorf("bucket: write: %w", err)
			}
			delay = min(delay*2, backoffCap)
		default:
			return false, fmt.Errorf("bucket: write: %w", err)
		}
	}
}

// Version is the ETag, which names the bytes: the same bytes stored again have the same ETag.
func (b s3Bucket) Version(ctx context.Context, key string) (string, error) {
	out, err := b.c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &b.name, Key: &key})
	var nf *types.NotFound
	switch {
	case errors.As(err, &nf) || status(err) == http.StatusNotFound:
		return "", bucket.ErrNotFound
	case err != nil:
		return "", fmt.Errorf("bucket: version: %w", err)
	}
	return aws.ToString(out.ETag), nil
}

// Delete ignores the version. DeleteObject takes If-Match with an ETag, but a file that a writer stores again has the
// bytes, and so the ETag, of the file that was deleted, so the precondition cannot tell them apart. The mark lease of
// the collector is the only guard of this backend.
func (b s3Bucket) Delete(ctx context.Context, key, _ string) error {
	_, err := b.c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &b.name, Key: &key})
	if err != nil && status(err) != http.StatusNotFound {
		return fmt.Errorf("bucket: delete: %w", err)
	}
	return nil
}

func (b s3Bucket) Get(ctx context.Context, key string) ([]byte, error) {
	return b.read(ctx, key, nil, -1)
}

func (b s3Bucket) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if off < 0 {
		return nil, fmt.Errorf("bucket: read: negative offset %d", off)
	}
	spec := fmt.Sprintf("bytes=%d-", off)
	if n >= 0 && off <= math.MaxInt64-n {
		spec = fmt.Sprintf("bytes=%d-%d", off, off+max(n, 1)-1)
	}
	return b.read(ctx, key, &spec, n)
}

func (b s3Bucket) read(ctx context.Context, key string, rng *string, n int64) ([]byte, error) {
	out, err := b.c.GetObject(ctx, &s3.GetObjectInput{Bucket: &b.name, Key: &key, Range: rng})
	var nsk *types.NoSuchKey
	switch {
	case errors.As(err, &nsk) || status(err) == http.StatusNotFound:
		return nil, bucket.ErrNotFound
	case status(err) == http.StatusRequestedRangeNotSatisfiable:
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("bucket: read: %w", err)
	}
	defer func() { _ = out.Body.Close() }()
	var r io.Reader = out.Body
	if n >= 0 {
		r = io.LimitReader(r, n)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("bucket: read: %w", err)
	}
	return data, nil
}
