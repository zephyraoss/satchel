package objectstore

import (
	"context"
	"errors"
)

var (
	ErrNotFound           = errors.New("objectstore: object not found")
	ErrPreconditionFailed = errors.New("objectstore: precondition failed")
)

type Object struct {
	Data []byte
	ETag string
}

type Store interface {
	Get(ctx context.Context, key string) (Object, error)
	PutIfAbsent(ctx context.Context, key string, data []byte) (etag string, err error)
	PutIfMatch(ctx context.Context, key string, data []byte, etag string) (newETag string, err error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
	DeletePrefix(ctx context.Context, prefix string) error
}
