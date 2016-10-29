package blob_cache

import (
	"s3proxy/source"
	"s3proxy/faulting"
	"github.com/karlseguin/ccache"
	"sync"
)

type BlobCache interface {
	Get(string) (*faulting.FaultingReader, error)
	GetMeta(string) *source.Meta
	Invalidate(string)
}

type S3Cache struct {
	source			source.UpstreamSource
	cachedFiles		map[string]*CacheEntry
	blockCache		*ccache.Cache
	lock            sync.Mutex
}

type CacheEntry struct {
	key          string
	meta         *source.Meta
	metaPath     string
	filePath     string
	faultingFile *faulting.FaultingFile
}

func NewS3Cache(s source.UpstreamSource) *S3Cache {
	c := make(map[string]*CacheEntry)
	return &S3Cache{
		source: s,
		cachedFiles: c,
	}
}

func (this S3Cache) Get(uri string) (*faulting.FaultingReader, error) {
	if entry, ok := this.cachedFiles[uri]; ok {
		return faulting.NewFaultingReader(entry.faultingFile), nil
	}

	this.lock.Lock()
	defer this.lock.Unlock()

	// Once we have the lock, make sure someone else didn't already do this
	// while we were waiting.
	if entry, ok := this.cachedFiles[uri]; ok {
		return faulting.NewFaultingReader(entry.faultingFile), nil
	}

	faultingFile, meta, err := this.source.Get(uri)
	if err != nil {
		return nil, err
	}

	entry := &CacheEntry{
		key: uri,
		meta: meta,
		faultingFile: faultingFile,
	}

	this.cachedFiles[uri] = entry

	return faulting.NewFaultingReader(faultingFile), nil
}

func (this S3Cache) GetMeta(uri string) *source.Meta {
	if entry, ok := this.cachedFiles[uri]; ok {
		return entry.meta
	}
	return nil
}

func (this S3Cache) Invalidate(uri string) {
}
