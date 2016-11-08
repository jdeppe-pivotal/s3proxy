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
)

var log = logging.MustGetLogger("s3proxy")

type BlobCache interface {
	Get(string) (*faulting.FaultingReader, error)
	GetMeta(string) *source.Meta
	Invalidate(string)
}

type S3Cache struct {
	source      source.UpstreamSource
	cachedFiles map[string]*CacheEntry
	cacheDir    string
	lock        sync.RWMutex
	ttl         int
	blockCache  *ccache.LayeredCache
}

type CacheEntry struct {
	key          string
	meta         *source.Meta
	faultingFile *faulting.FaultingFile
}

func NewS3Cache(cache *ccache.LayeredCache, s source.UpstreamSource, cacheDir string, ttl int) *S3Cache {
	c := make(map[string]*CacheEntry)

	return &S3Cache{
		source: s,
		cachedFiles: c,
		cacheDir: cacheDir,
		ttl: ttl,
		blockCache: cache,
	}
}

func (this S3Cache) Get(uri string) (*faulting.FaultingReader, error) {
	this.validateEntry(uri)

	this.lock.RLock()
	if entry, ok := this.cachedFiles[uri]; ok {
		log.Debugf("Cache hit: %s", uri)
		return faulting.NewFaultingReader(entry.faultingFile), nil
	}
	this.lock.RUnlock()

	this.lock.Lock()
	defer this.lock.Unlock()

	// Once we have the lock, make sure someone else didn't already do this
	// while we were waiting.
	if entry, ok := this.cachedFiles[uri]; ok {
		log.Debugf("Cache hit: %s", uri)
		return faulting.NewFaultingReader(entry.faultingFile), nil
	}

	log.Debugf("Cache miss: %s", uri)
	faultingFile, meta, err := this.source.Get(uri)
	if err != nil {
		return nil, err
	}

	// Set the TTL
	meta.Expires = time.Now().Add(time.Duration(this.ttl) * time.Second)

	err = writeMeta(meta, faultingFile.Dst)
	if err != nil {
		log.Errorf("ERROR saving meta: %s", err)
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
	this.lock.RLock()
	defer this.lock.RUnlock()
	if entry, ok := this.cachedFiles[uri]; ok {
		return entry.meta
	}
	return nil
}

func (this S3Cache) Invalidate(uri string) {
}

func (this S3Cache) RecoverMeta() {
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
			log.Debugf("  -> %+v", m)
			this.addMeta(m, objectPath)
		}
		return nil
	})
}

func (this S3Cache) addMeta(meta *source.Meta, objectPath string) {
	dst := path.Join(this.cacheDir, objectPath)
	cc := this.blockCache.GetOrCreateSecondaryCache(objectPath)
	ff, err := faulting.NewFaultingFile(nil, dst, meta.Size, cc)
	if err != nil {
		log.Errorf("Unable to recover meta for %s", objectPath)
		return
	}
	ff.BlockCount = int((ff.Size / int64(ff.BlockSize)) + 1)

	entry := &CacheEntry{
		key: objectPath,
		meta: meta,
		faultingFile: ff,
	}
	this.cachedFiles[objectPath] = entry
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

func (this S3Cache) validateEntry(uri string) {
	// Early out if we're not currently caching this object
	entry := this.cachedFiles[uri]
	if entry == nil {
		return
	}

	// Has this entry already expired?
	if entry.meta.Expires.After(time.Now()) {
		return
	}

	// Get current Meta
	meta := this.source.GetMeta(uri)

	// Check the ETag, Size and LastModified
	if meta.ETag == entry.meta.ETag &&
			meta.Size == entry.meta.Size &&
			meta.LastModified == entry.meta.LastModified {
		return
	}

	// If there is a change, then remove the currently cached entry
	log.Debugf("Expiring %s", uri)
	this.lock.Lock()
	defer this.lock.Unlock()
	delete(this.cachedFiles, uri)
}
