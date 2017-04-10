package fakes

import (
	"s3proxy/faulting"
	"s3proxy/source"
	"strings"
	"strconv"
	"io"
	"fmt"
	"errors"
	"path"
	"github.com/karlseguin/ccache"
	"time"
	"context"
)

type FakeUpstreamSource struct {
	baseDir        string
	cacheBlockSize int
	blockCache     *ccache.LayeredCache
}

func NewFakeUpstreamSource(baseDir string, cache *ccache.LayeredCache) *FakeUpstreamSource {
	return &FakeUpstreamSource{
		baseDir: baseDir,
		cacheBlockSize: 0,
		blockCache: cache,
	}
}

func (this *FakeUpstreamSource) Get(ctx context.Context, uri string) (*faulting.FaultingFile, *source.Meta, error) {
	parts := strings.Split(strings.TrimLeft(uri, "/"), "/")
	size, _ := strconv.Atoi(parts[len(parts) - 1])

	cachedFile := path.Join(this.baseDir, path.Join(parts...))

	var r GeneratedContentReader

	switch parts[0] {
	case "error":
		r = NewErroringSource(size)
	case "uncached":
		cachedFile = "/dev/null"
		r = NewIntegerStreamingSource(size)
	default:
		r = NewIntegerStreamingSource(size)
	}

	secondaryCache := this.blockCache.GetOrCreateSecondaryCache(uri)
	ff, err := faulting.NewFaultingFile(r, cachedFile, r.Size(), secondaryCache)
	if err != nil {
		return nil, nil, err
	}

	if this.cacheBlockSize > 0 {
		ff.SetBlockSize(this.cacheBlockSize)
	}
	ff.Stream(nil)
	meta := &source.Meta{
		Size: ff.Size,
	}

	return ff, meta, nil
}

func (this *FakeUpstreamSource) GetMeta(uri string) (*source.Meta, error) {
	return &source.Meta{}, nil
}

func (this *FakeUpstreamSource) Directory(dir string) ([]string, error) {
	return []string{}, nil
}

type GeneratedContentReader interface {
	io.Reader
	Size()      int64
}

type ErroringSource struct {
	Content []byte
	offset  int
	closed  bool
}

// Errors after sending half the desired size
func NewErroringSource(size int) *ErroringSource {
	c := make([]byte, 0)
	for i := 0; i < size; i++ {
		c = append(c, fmt.Sprintf("%d ", i)...)
	}

	return &ErroringSource{
		Content: c,
		offset: 0,
		closed: false,
	}
}

func (this *ErroringSource) Size() int64 {
	return int64(len(this.Content))
}

func (this *ErroringSource) Read(p []byte) (int, error) {
	if this.closed {
		return 0, errors.New("Read failed: source is closed")
	}

	if this.offset > len(this.Content) / 2 {
		return 0, errors.New("Failed to read more than half")
	}

	if this.offset == len(this.Content) {
		return 0, io.EOF
	}

	n := copy(p, this.Content[this.offset:])
	this.offset += n

	time.Sleep(100 * time.Millisecond)

	return n, nil
}

func (this *ErroringSource) GetMeta(uri string) (*source.Meta, error) {
	return &source.Meta{}, nil
}

type IntegerSequenceSource struct {
	Content []byte
	offset  int
	closed  bool
}

func NewIntegerStreamingSource(size int) *IntegerSequenceSource {
	c := make([]byte, 0)
	for i := 0; i < size; i++ {
		c = append(c, fmt.Sprintf("%d ", i)...)
	}

	return &IntegerSequenceSource{
		Content: c,
		offset: 0,
		closed: false,
	}
}

func (this *IntegerSequenceSource) Size() int64 {
	return int64(len(this.Content))
}

func (this *IntegerSequenceSource) Read(p []byte) (int, error) {
	if this.closed {
		return 0, errors.New("Read failed: source is closed")
	}

	if this.offset == len(this.Content) {
		return 0, io.EOF
	}

	n := copy(p, this.Content[this.offset:])
	this.offset += n

	return n, nil
}

func (this *IntegerSequenceSource) Close() error {
	this.closed = true
	return nil
}

func (this *IntegerSequenceSource) GetMeta(uri string) (*source.Meta, error) {
	return &source.Meta{}, nil
}

