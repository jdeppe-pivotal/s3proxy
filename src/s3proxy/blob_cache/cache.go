package blob_cache

import (
	"s3proxy/source"
	"s3proxy/faulting"
	"sync"
	"time"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"github.com/op/go-logging"
	"strings"
	"path/filepath"
	"github.com/karlseguin/ccache"
	"path"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"s3proxy/context"
	"golang.org/x/net/context"
)

var log = logging.MustGetLogger("s3proxy")

type BlobCache interface {
	Get(context.Context, string) (*faulting.FaultingReader, error)
	GetMeta(string) *source.Meta
	Delete(context.Context, string)
	Directory(string) ([]string, error)
}

type S3Cache struct {
	sync.RWMutex
	source      source.UpstreamSource
	cachedFiles map[string]*cacheEntryWrapper
	cacheDir    string
	ttl         int
	blockCache  *ccache.LayeredCache
}

type cacheEntry struct {
	key          string
	meta         *source.Meta
	faultingFile *faulting.FaultingFile
}

type cacheEntryWrapper struct {
	sync.RWMutex
	entry *cacheEntry
}

func NewS3Cache(cache *ccache.LayeredCache, s source.UpstreamSource, cacheDir string, ttl int) *S3Cache {
	c := make(map[string]*cacheEntryWrapper)

	return &S3Cache{
		source: s,
		cachedFiles: c,
		cacheDir: cacheDir,
		ttl: ttl,
		blockCache: cache,
	}
}

func (this *S3Cache) Get(ctx context.Context, uri string) (*faulting.FaultingReader, error) {
	this.validateEntry(ctx, uri)

	ctxValue := ctx.Value(0).(*cache_context.Context)

	this.RLock()
	if wrapper, ok := this.cachedFiles[uri]; ok && wrapper.entry != nil {
		log.Debugf("[%d] Cache hit: %s", ctxValue.Sequence, uri)
		this.RUnlock()
		return faulting.NewFaultingReader(ctx, wrapper.entry.faultingFile), nil
	}
	this.RUnlock()

	this.Lock()
	defer this.Unlock()

	// Once we have the lock, make sure someone else didn't already do this
	// while we were waiting.
	if wrapper, ok := this.cachedFiles[uri]; ok && wrapper.entry != nil {
		log.Debugf("[%d] Cache hit: %s", ctxValue.Sequence, uri)
		return faulting.NewFaultingReader(ctx, wrapper.entry.faultingFile), nil
	}

	log.Debugf("[%d] Cache miss: %s", ctxValue.Sequence, uri)
	faultingFile, meta, err := this.source.Get(ctx, uri)
	if err != nil {
		return nil, err
	}

	// Set the TTL
	meta.Expires = time.Now().Add(time.Duration(this.ttl) * time.Second)

	err = writeMeta(meta, faultingFile.Dst)
	if err != nil {
		log.Errorf("[%d] ERROR saving meta: %s", ctxValue.Sequence, err)
	}

	entry := &cacheEntry{
		key: uri,
		meta: meta,
		faultingFile: faultingFile,
	}

	if wrapper, ok := this.cachedFiles[uri]; ok {
		wrapper.entry = entry
	} else {
		this.cachedFiles[uri] = &cacheEntryWrapper{
			entry: entry,
		}
	}

	return faulting.NewFaultingReader(ctx, faultingFile), nil
}

func (this *S3Cache) GetMeta(uri string) *source.Meta {
	this.RLock()
	defer this.RUnlock()
	if wrapper, ok := this.cachedFiles[uri]; ok && wrapper.entry != nil {
		return wrapper.entry.meta
	}
	return nil
}

func (this *S3Cache) RecoverMeta() {
	filepath.Walk(this.cacheDir, func(path string, info os.FileInfo, err error) error {
		if strings.Contains(path, "_meta_") {
			metaJson, err := ioutil.ReadFile(path)
			if err != nil {
				log.Errorf("Unable to process meta from %s - %s", path, err)
				return err
			}

			m := &source.Meta{}
			err = json.Unmarshal(metaJson, m)
			if err != nil {
				log.Errorf("Unable to unmarshal meta from %s - %s", path, err)
				return err
			}

			objectPath := strings.TrimPrefix(path, this.cacheDir)
			objectPath = strings.TrimSuffix(objectPath, "._meta_")
			log.Debugf("Adding meta %s", objectPath)
			log.Debugf("    %+v", m)
			this.AddMeta(m, objectPath)
		}
		return nil
	})
}

func (this *S3Cache) AddMeta(meta *source.Meta, objectPath string) {
	dst := path.Join(this.cacheDir, objectPath)
	cc := this.blockCache.GetOrCreateSecondaryCache(objectPath)
	ff, err := faulting.NewFaultingFile(nil, dst, meta.Size, cc)
	if err != nil {
		log.Errorf("Unable to recover meta for %s", objectPath)
		return
	}
	ff.BlockCount = int((ff.Size / int64(ff.BlockSize)) + 1)

	entry := &cacheEntry{
		key: objectPath,
		meta: meta,
		faultingFile: ff,
	}
	this.cachedFiles[objectPath] = &cacheEntryWrapper{
		entry: entry,
	}
}

func writeMeta(meta *source.Meta, objectFile string) error {
	metaJson, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	metaFile := fmt.Sprintf("%s._meta_", objectFile)
	err = ioutil.WriteFile(metaFile, metaJson, os.ModePerm|0755)
	if err != nil {
		return err
	}

	return nil
}

func (this *S3Cache) validateEntry(ctx context.Context, uri string) {
	// Early out if we're not currently caching this object
	this.RLock()
	wrapper, found := this.cachedFiles[uri]
	this.RUnlock()

	ctxValue := ctx.Value(0).(*cache_context.Context)

	if ! found || wrapper.entry == nil {
		return
	}

	// Has this entry already expired?
	if wrapper.entry.meta.Expires.After(time.Now()) {
		return
	}

	wrapper.Lock()
	defer wrapper.Unlock()

	// Somebody else might have done this while we were waiting for the lock
	if wrapper.entry == nil {
		return
	}

	// Get current Meta
	meta, err := this.source.GetMeta(uri)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "NotFound" {
				log.Infof("[%d] Upstream not found for %s", ctxValue.Sequence, uri)
				this.Delete(ctx, uri)
			}
		} else {
			log.Debugf("[%d] Unable to get meta: %s", ctxValue.Sequence, err)
		}
		return
	}

	// Check the ETag, Size and LastModified
	if meta.ETag == wrapper.entry.meta.ETag &&
			meta.Size == wrapper.entry.meta.Size &&
			meta.LastModified == wrapper.entry.meta.LastModified {
		wrapper.entry.meta.Expires = time.Now().Add(time.Duration(this.ttl) * time.Second)
		log.Infof("[%d] Revalidated %s", ctxValue.Sequence, uri)
		return
	}

	// If there is a change, then remove the currently cached entry
	log.Debugf("[%d] Expiring %s", ctxValue.Sequence, uri)
	this.Delete(ctx, uri)
}

func (this *S3Cache) Delete(ctx context.Context, uri string) {
	ctxValue := ctx.Value(0).(*cache_context.Context)

	this.RLock()
	defer this.RUnlock()

	if wrapper, ok := this.cachedFiles[uri]; ok {
		if wrapper.entry == nil {
			return
		}
		log.Debugf("[%d] Deleting entry for request %s -> %s", ctxValue.Sequence, uri, wrapper.entry.faultingFile.Dst)

		meta := fmt.Sprintf("%s._meta_", wrapper.entry.faultingFile.Dst)
		os.Remove(wrapper.entry.faultingFile.Dst)
		os.Remove(meta)
		this.blockCache.DeleteAll(uri)

		wrapper.entry = nil
	}
}

func (this *S3Cache) Directory(path string) ([]string, error) {
	return this.source.Directory(path)
}