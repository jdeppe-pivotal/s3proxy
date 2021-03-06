package proxy

import (
	"net/http"
	"io"
	"s3proxy/blob_cache"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/op/go-logging"
	"strings"
	"net"
	"sync/atomic"
	"s3proxy/context"
	"golang.org/x/net/context"
)

type S3Proxy struct {
	cache blob_cache.BlobCache
}

var requestCounter uint64
var log = logging.MustGetLogger("s3proxy")

func NewS3Proxy(c blob_cache.BlobCache) *S3Proxy {
	return &S3Proxy{c}
}

func (this *S3Proxy) Handler(w http.ResponseWriter, req *http.Request) {
	// Ugghhh. Hardcode...
	if req.URL.Path == "/favicon.ico" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// Create a simple context to pass down to other functions
	counter := atomic.AddUint64(&requestCounter, 1)
	ctxValue := &cache_context.Context {
		Sequence: counter,
	}
	ctx := context.WithValue(context.Background(), 0, ctxValue)

	log.Infof("[%d] Requesting %s", counter, req.URL.Path)

	if strings.HasSuffix(req.URL.Path, "/") {
		dirs, err := this.cache.Directory(req.URL.Path)
		if err != nil {
			log.Errorf("[%d] Unable to return directory: %s", counter, err)
		}

		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-type", "text/plain")
		for _, dir := range dirs {
			w.Write([]byte(dir))
			w.Write([]byte("\n"))
		}
		return
	}

	r, err := this.cache.Get(ctx, req.URL.Path)
	defer r.Close()

	if err != nil {
		code := http.StatusInternalServerError
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "NotFound" || awsErr.Code() == "NoSuchKey" {
				code = http.StatusNotFound
			} else {
				log.Errorf("[%d] AWS Unclassified error: %+v", counter, awsErr)
			}
		} else {
			log.Errorf("[%d] ERROR: %+v", counter, err)
		}
		w.WriteHeader(code)
		return
	}

	meta := this.cache.GetMeta(req.URL.Path)
	if meta != nil {
		w.Header().Set("Content-length", fmt.Sprintf("%d", meta.Size))
		w.Header().Set("Content-type", meta.ContentType)
	}

	_, err = io.Copy(w, r)
	if err != nil {
		// This is a bit messy, but we really don't care if the client aborted the
		// connection. Other errors are assumed to be from the upstream side and
		// thus result in the cache entry being removed.
		if e, ok := err.(*net.OpError); ok {
			if e.Op != "write" {
				log.Errorf("[%d] Error streaming %s: %s", counter, req.URL.Path, e.Err)
				this.cache.Delete(ctx, req.URL.Path)
			}
		} else {
			log.Errorf("[%d] Error streaming %s: %s", counter, req.URL.Path, err)
			this.cache.Delete(ctx, req.URL.Path)
		}
	}
}

func (this *S3Proxy) Delete(w http.ResponseWriter, req *http.Request) {
	counter := atomic.AddUint64(&requestCounter, 1)
	ctxValue := &cache_context.Context {
		Sequence: counter,
	}
	ctx := context.WithValue(context.Background(), 0, ctxValue)

	uri := req.URL.Path
	uri = strings.TrimPrefix(uri, "/admin")
	log.Infof("[%d] Deleted: %s", counter, uri)

	this.cache.Delete(ctx, uri)
	w.WriteHeader(http.StatusNoContent)
}
