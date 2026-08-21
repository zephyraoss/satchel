package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/google/uuid"
)

type S3Config struct {
	Endpoint       string
	Region         string
	Bucket         string
	AccessKeyID    string
	SecretKey      string
	ForcePathStyle bool
}

type S3 struct {
	client *s3.Client
	bucket string
}

func NewS3(cfg S3Config) *S3 {
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	client := s3.New(s3.Options{
		Region:       region,
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretKey, ""),
		BaseEndpoint: optionalString(cfg.Endpoint),
		UsePathStyle: cfg.ForcePathStyle,
	})
	return &S3{client: client, bucket: cfg.Bucket}
}

func optionalString(s string) *string {
	if s == "" {
		return nil
	}
	return aws.String(s)
}

func (s *S3) Get(ctx context.Context, key string) (Object, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		if isMissingKey(err) {
			return Object{}, ErrNotFound
		}
		return Object{}, err
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return Object{}, err
	}
	return Object{Data: data, ETag: aws.ToString(out.ETag)}, nil
}

func (s *S3) PutIfAbsent(ctx context.Context, key string, data []byte) (string, error) {
	return s.put(ctx, &s3.PutObjectInput{
		Bucket:      &s.bucket,
		Key:         &key,
		Body:        bytes.NewReader(data),
		IfNoneMatch: aws.String("*"),
	})
}

func (s *S3) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (string, error) {
	newETag, err := s.put(ctx, &s3.PutObjectInput{
		Bucket:  &s.bucket,
		Key:     &key,
		Body:    bytes.NewReader(data),
		IfMatch: aws.String(etag),
	})
	if isMissingKey(err) {
		return "", ErrPreconditionFailed
	}
	return newETag, err
}

func isMissingKey(err error) bool {
	var nsk *types.NoSuchKey
	return errors.As(err, &nsk) || httpStatus(err) == http.StatusNotFound
}

func (s *S3) put(ctx context.Context, in *s3.PutObjectInput) (string, error) {
	out, err := s.client.PutObject(ctx, in)
	if err != nil {
		if isPreconditionFailure(err) {
			return "", ErrPreconditionFailed
		}
		return "", err
	}
	return aws.ToString(out.ETag), nil
}

func isPreconditionFailure(err error) bool {
	switch httpStatus(err) {
	case http.StatusPreconditionFailed, http.StatusConflict, http.StatusNotModified:
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict", "NotModified":
			return true
		}
	}
	return false
}

func httpStatus(err error) int {
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		return respErr.HTTPStatusCode()
	}
	return 0
}

func (s *S3) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: &key})
	return err
}

func (s *S3) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}
	return keys, nil
}

func (s *S3) DeletePrefix(ctx context.Context, prefix string) error {
	keys, err := s.List(ctx, prefix)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := s.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func VerifyConditionalWrites(ctx context.Context, store Store) error {
	probe := "leases/.probe-" + uuid.NewString()
	defer store.Delete(context.WithoutCancel(ctx), probe)

	etag, err := store.PutIfAbsent(ctx, probe, []byte("probe"))
	if err != nil {
		return fmt.Errorf("probe write: %w", err)
	}
	if _, err := store.PutIfAbsent(ctx, probe, []byte("probe")); !errors.Is(err, ErrPreconditionFailed) {
		return fmt.Errorf("backend accepted If-None-Match PUT on an existing key (err=%v); conditional writes are required for the lease", err)
	}
	if _, err := store.PutIfMatch(ctx, probe, []byte("probe"), "\"bogus\""); !errors.Is(err, ErrPreconditionFailed) {
		return fmt.Errorf("backend accepted If-Match PUT with a stale ETag (err=%v); conditional writes are required for the lease", err)
	}
	if _, err := store.PutIfMatch(ctx, probe, []byte("probe"), etag); err != nil {
		return fmt.Errorf("If-Match PUT with the current ETag failed: %w", err)
	}
	return nil
}

func (s *S3) Fetch(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}
