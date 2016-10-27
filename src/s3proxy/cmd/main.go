package main

import (
	"net/http"
	"s3proxy/proxy"
	"s3proxy/source"
	"s3proxy/blob_cache"
)

func main() {
	s := source.NewS3Source()
	c := blob_cache.NewS3Cache(*s)
	poxy := proxy.NewS3Proxy(c)
	http.HandleFunc("/", poxy.Handler)
	http.ListenAndServe(":8080", nil)
}
