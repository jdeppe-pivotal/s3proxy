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
	"strings"
	"github.com/go-zoo/bone"
)

var log = logging.MustGetLogger("s3proxy")

type Config struct {
	port      int
	cacheSize int64
	cacheDir  string
	region    string
	ttl       int
}

func init() {
	format := logging.MustStringFormatter(
		`%{color}%{time:2006-01-02 15:04:05.000} %{shortfunc} ▶ %{level:.4s}%{color:reset} %{message}`,
	)
	backend1 := logging.NewLogBackend(os.Stdout, "", 0)
	formatter := logging.NewBackendFormatter(backend1, format)
	logging.SetBackend(formatter)
}

func main() {
	config := processArgs()

	cache := ccache.Layered(ccache.Configure().MaxSize(config.cacheSize).ItemsToPrune(100))
	s := source.NewS3Source(cache, config.region, config.cacheDir)
	c := blob_cache.NewS3Cache(cache, *s, config.cacheDir, config.ttl)

	log.Info("Scanning for meta files")
	c.RecoverMeta()

	pxy := proxy.NewS3Proxy(c)

	m := bone.New()

	m.Delete("/*", http.HandlerFunc(pxy.Delete))
	m.Get("/*", http.HandlerFunc(pxy.Handler))

	//http.HandleFunc("/", pxy.Handler)
	http.ListenAndServe(fmt.Sprintf(":%d", config.port), m)
}

//func Admin(w http.ResponseWriter, req *http.Request) {
//	log.Info("admin called")
//}

func processArgs() *Config {
	c := &Config{}

	flag.StringVar(&c.cacheDir, "c", ".", "cache directory")
	flag.Int64Var(&c.cacheSize, "m", 1000, "size of in-memory cache (in MB)")
	flag.IntVar(&c.port, "p", 8080, "port to listen on")
	flag.StringVar(&c.region, "r", "us-west-2", "region to use")
	flag.IntVar(&c.ttl, "t", 600, "time before objects are re-validated (in seconds)")

	flag.Parse()

	if c.cacheDir == "." {
		var err error
		c.cacheDir, err = os.Getwd()
		if err != nil {
			log.Fatalf("Unable to determine pwd: %v", err)
		}
	}

	// Don't want this dir to end with a / - it messes up other things.
	c.cacheDir = strings.TrimRight(c.cacheDir, "/")

	log.Infof("Starting s3proxy with:")
	log.Infof("    port:            %d", c.port)
	log.Infof("    cache size (MB): %d", c.cacheSize)
	log.Infof("    time-to-live:    %d", c.ttl)
	log.Infof("    region:          %s", c.region)
	log.Infof("    cache dir:       %s", c.cacheDir)

	return c
}
