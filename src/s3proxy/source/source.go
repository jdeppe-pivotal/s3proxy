package source

import (
	"time"
	"s3proxy/faulting"
	"context"
)

type Meta struct {
	Expires      time.Time  `json:"expires"`
	LastModified time.Time  `json:"last_modified"`
	Size         int64      `json:"size"`
	ContentType  string     `json:"content_type"`
	ETag         string     `json:"etag"`
}

type UpstreamSource interface {
	Get(ctx context.Context, uri string) (*faulting.FaultingFile, *Meta, error)
	GetMeta(uri string) (*Meta, error)
	Directory(path string) ([]string, error)
}
