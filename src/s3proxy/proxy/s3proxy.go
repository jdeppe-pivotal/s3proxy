package proxy

import (
	"net/http"
	"io"
	"s3proxy/blob_cache"
	"fmt"
)

type S3Proxy struct {
	cache blob_cache.BlobCache
}

func NewS3Proxy(c blob_cache.BlobCache) *S3Proxy {
	return &S3Proxy{c}
}

func (this *S3Proxy) Handler(w http.ResponseWriter, req *http.Request) {
	r, err := this.cache.Get(req.URL.Path)
	defer r.Close()

	fmt.Printf("-> Requesting %s\n", req.URL.Path)

	if err != nil {
		//if awsErr, ok := err.(awserr.Error); ok {
		//	fmt.Printf("ERROR: %+v\n", awsErr)
		//}
		fmt.Printf("ERROR: %+v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-length", fmt.Sprintf("%d", r.Size()))
	w.Header().Set("Content-type", "text/html")

	io.Copy(w, r)
}
