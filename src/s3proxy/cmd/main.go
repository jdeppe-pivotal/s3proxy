package main

import (
	"net/http"
	"s3proxy/proxy"
	"s3proxy/source"
	"s3proxy/blob_cache"
	"github.com/karlseguin/ccache"
	"flag"
	"fmt"
	"github.com/op/go-logging"
	"os"
)

var port int
var cacheSize int64
var cacheDir string
var region string

var log = logging.MustGetLogger("s3proxy")

func init() {
	format := logging.MustStringFormatter(
		`%{color}%{time:2006-01-02 15:04:05.000} %{shortfunc} ▶ %{level:.4s}%{color:reset} %{message}`,
	)
	backend1 := logging.NewLogBackend(os.Stdout, "", 0)
	formatter := logging.NewBackendFormatter(backend1, format)
	logging.SetBackend(formatter)
}

func main() {
	flag.IntVar(&port, "-p", 8080, "port to listen on")
	flag.Int64Var(&cacheSize, "-m", 1000, "size of in-memory cache (in MB)")
	flag.StringVar(&cacheDir, "-c", ".", "cache directory")
	flag.StringVar(&region, "-r", "us-west-2", "Region to use")

	flag.Parse()

	log.Infof("Starting s3proxy with:")
	log.Infof("    port:            %d", port)
	log.Infof("    cache size (MB): %d", cacheSize)
	log.Infof("    region:          %s", region)
	log.Infof("    cache dir:       %s", cacheDir)

	cache := ccache.Layered(ccache.Configure().MaxSize(cacheSize).ItemsToPrune(100))
	s := source.NewS3Source(cache, region, cacheDir)
	c := blob_cache.NewS3Cache(*s)
	pxy := proxy.NewS3Proxy(c)

	http.HandleFunc("/", pxy.Handler)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
