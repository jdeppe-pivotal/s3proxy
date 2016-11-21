package proxy

import (
	"net/http"
	"io"
	"s3proxy/blob_cache"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/op/go-logging"
	"strings"
)

type S3Proxy struct {
	cache blob_cache.BlobCache
}

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

	r, err := this.cache.Get(req.URL.Path)
	meta := this.cache.GetMeta(req.URL.Path)

	defer r.Close()

	if err != nil {
		code := http.StatusInternalServerError
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "NotFound" {
				code = http.StatusNotFound
			} else {
				log.Errorf("AWS Unclassified error: %+v", awsErr)
			}
		} else {
			log.Errorf("ERROR: %+v", err)
		}
		w.WriteHeader(code)
		return
	}

	if meta != nil {
		w.Header().Set("Content-length", fmt.Sprintf("%d", meta.Size))
		w.Header().Set("Content-type", meta.ContentType)
	}

	io.Copy(w, r)
}

func (this *S3Proxy) Delete(w http.ResponseWriter, req *http.Request) {
	uri := req.URL.Path
	uri = strings.TrimPrefix(uri, "/admin")
	log.Infof("Deleted: %s", uri)

	this.cache.Delete(uri)
	w.WriteHeader(http.StatusNoContent)
}
