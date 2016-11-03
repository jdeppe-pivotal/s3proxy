package main

import (
	"net/http"
	"s3proxy/proxy"
	"s3proxy/source"
	"s3proxy/blob_cache"
	"github.com/karlseguin/ccache"
)

func main() {
	cache := ccache.Layered(ccache.Configure().MaxSize(1000).ItemsToPrune(100))
	s := source.NewS3Source(cache)
	c := blob_cache.NewS3Cache(*s)
	poxy := proxy.NewS3Proxy(c)
	http.HandleFunc("/", poxy.Handler)
	http.ListenAndServe(":8080", nil)
}
