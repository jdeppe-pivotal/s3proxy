package proxy

import (
	"net/http"
	"io"
	"s3proxy/blob_cache"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
)

type S3Proxy struct {
	cache blob_cache.BlobCache
}

func NewS3Proxy(c blob_cache.BlobCache) *S3Proxy {
	return &S3Proxy{c}
}

func (this *S3Proxy) Handler(w http.ResponseWriter, req *http.Request) {
	fmt.Printf("-> Requesting %s\n", req.URL.Path)

	r, err := this.cache.Get(req.URL.Path)
	meta := this.cache.GetMeta(req.URL.Path)
	defer r.Close()

	if err != nil {
		code := http.StatusInternalServerError
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "NotFound" {
				code = http.StatusNotFound
			} else {
				fmt.Printf("AWS Unclassified error: %+v\n", awsErr)
			}
		} else {
			fmt.Printf("ERROR: %+v\n", err)
		}
		w.WriteHeader(code)
		return
	}

	w.Header().Set("Content-length", fmt.Sprintf("%d", r.Size()))
	if meta != nil {
		w.Header().Set("Content-type", meta.ContentType)
	}

	io.Copy(w, r)
}
