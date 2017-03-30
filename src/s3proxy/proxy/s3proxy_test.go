package proxy_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"net/http/httptest"
	"io/ioutil"
	"s3proxy/fakes"
	"s3proxy/proxy"
	"s3proxy/blob_cache"
	"os"
	"path"
	"github.com/op/go-logging"
	"github.com/karlseguin/ccache"
)

var log = logging.MustGetLogger("s3proxy")

var _ = Describe("S3Proxy test suite", func() {
	Context("regular proxy URLs", func() {
		It("produces the correct result", func() {
			cacheDir, err := ioutil.TempDir("", "cached-")
			Expect(err).To(BeNil())
			defer os.RemoveAll(cacheDir)

			bc := ccache.Layered(ccache.Configure())
			fus := fakes.NewFakeUpstreamSource(cacheDir, bc)
			cache := blob_cache.NewS3Cache(bc, fus, cacheDir, 60)
			p := proxy.NewS3Proxy(cache)

			handler := http.HandlerFunc(p.Handler)
			req, err := http.NewRequest("GET", "/test_bucket/10", nil)
			Expect(err).To(BeNil())

			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			Expect(rr.Code).To(Equal(http.StatusOK))

			Expect(rr.Header().Get("Content-length")).To(Equal("20"))

			body, err := ioutil.ReadAll(rr.Body)
			Expect(err).To(BeNil())
			Expect(string(body)).To(Equal("0 1 2 3 4 5 6 7 8 9 "))

			cachedData, err := ioutil.ReadFile(path.Join(cacheDir, "test_bucket", "10"))
			Expect(err).To(BeNil())
			Expect(string(cachedData)).To(Equal("0 1 2 3 4 5 6 7 8 9 "))

			Expect(bc.Get("/test_bucket/10", "0")).ToNot(BeNil())
		})

		It("produces a cache file", func() {
			cacheDir, err := ioutil.TempDir("", "cached-")
			Expect(err).To(BeNil())
			defer os.RemoveAll(cacheDir)

			bc := ccache.Layered(ccache.Configure())
			fus := fakes.NewFakeUpstreamSource(cacheDir, bc)
			cache := blob_cache.NewS3Cache(bc, fus, cacheDir, 60)
			p := proxy.NewS3Proxy(cache)

			handler := http.HandlerFunc(p.Handler)
			req, err := http.NewRequest("GET", "/test_bucket/10", nil)
			Expect(err).To(BeNil())

			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			Expect(rr.Code).To(Equal(http.StatusOK))

			body, err := ioutil.ReadAll(rr.Body)
			Expect(err).To(BeNil())
			Expect(string(body)).To(Equal("0 1 2 3 4 5 6 7 8 9 "))

			cachedData, err := ioutil.ReadFile(path.Join(cacheDir, "test_bucket", "10"))
			Expect(err).To(BeNil())
			Expect(string(cachedData)).To(Equal("0 1 2 3 4 5 6 7 8 9 "))
		})

		It("can stream > 10MB", func() {
			cacheDir, err := ioutil.TempDir("", "cached-")
			Expect(err).To(BeNil())
			defer os.RemoveAll(cacheDir)

			bc := ccache.Layered(ccache.Configure())
			fus := fakes.NewFakeUpstreamSource(cacheDir, bc)
			cache := blob_cache.NewS3Cache(bc, fus, cacheDir, 60)
			p := proxy.NewS3Proxy(cache)

			handler := http.HandlerFunc(p.Handler)
			req, err := http.NewRequest("GET", "/uncached/2000000", nil)
			Expect(err).To(BeNil())

			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			Expect(rr.Code).To(Equal(http.StatusOK))
			Expect(rr.Header().Get("Content-length")).To(Equal("14888890"))

			body, err := ioutil.ReadAll(rr.Body)
			Expect(err).To(BeNil())
			Expect(len(body)).To(Equal(14888890))
		})

		It("handles errors correctly", func() {
			cacheDir, err := ioutil.TempDir("", "cached-")
			Expect(err).To(BeNil())
			defer os.RemoveAll(cacheDir)

			bc := ccache.Layered(ccache.Configure())
			fus := fakes.NewFakeUpstreamSource(cacheDir, bc)
			cache := blob_cache.NewS3Cache(bc, fus, cacheDir, 60)
			p := proxy.NewS3Proxy(cache)

			handler := http.HandlerFunc(p.Handler)
			req, err := http.NewRequest("GET", "/error/500000", nil)
			Expect(err).To(BeNil())

			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			Expect(rr.Code).To(Equal(http.StatusOK))

			Expect(rr.Header().Get("Content-length")).To(Equal("3388890"))

			body, err := ioutil.ReadAll(rr.Body)
			Expect(err).To(BeNil())
			Expect(len(body) > 0).To(BeTrue())
			Expect(len(body) < 3000000).To(BeTrue())
		})

		It("cleans up correctly after an error", func() {
			cacheDir, err := ioutil.TempDir("", "cached-")
			Expect(err).To(BeNil())
			defer os.RemoveAll(cacheDir)

			bc := ccache.Layered(ccache.Configure())
			fus := fakes.NewFakeUpstreamSource(cacheDir, bc)
			cache := blob_cache.NewS3Cache(bc, fus, cacheDir, 60)
			p := proxy.NewS3Proxy(cache)

			handler := http.HandlerFunc(p.Handler)
			req, err := http.NewRequest("GET", "/error/500000", nil)
			Expect(err).To(BeNil())

			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			Expect(rr.Code).To(Equal(http.StatusOK))

			Expect(rr.Header().Get("Content-length")).To(Equal("3388890"))

			body, err := ioutil.ReadAll(rr.Body)
			Expect(err).To(BeNil())
			Expect(len(body) > 0).To(BeTrue())
			Expect(len(body) < 3000000).To(BeTrue())

			_, err = os.Stat(path.Join(cacheDir, "error", "500000"))
			Expect(err).ToNot(BeNil())

			_, err = os.Stat(path.Join(cacheDir, "error", "500000._meta_"))
			Expect(err).ToNot(BeNil())

			Expect(bc.Get("/error/500000", "0")).To(BeNil())
		})

		It("recovers meta files", func() {
			cacheDir, err := ioutil.TempDir("", "cached-")
			Expect(err).To(BeNil())
			defer os.RemoveAll(cacheDir)

			bc := ccache.Layered(ccache.Configure())
			fus := fakes.NewFakeUpstreamSource(cacheDir, bc)
			cache := blob_cache.NewS3Cache(bc, fus, cacheDir, 60)
			p := proxy.NewS3Proxy(cache)

			handler := http.HandlerFunc(p.Handler)
			req, err := http.NewRequest("GET", "/test_bucket/10", nil)
			Expect(err).To(BeNil())

			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			body1, err := ioutil.ReadAll(rr.Body)
			Expect(err).To(BeNil())

			// Create a new cache but pass in a nil source.
			// This would cause a panic if the caching doesn't actually work.
			bc2 := ccache.Layered(ccache.Configure())
			cache2 := blob_cache.NewS3Cache(bc2, nil, cacheDir, 60)
			cache2.RecoverMeta()

			m := cache2.GetMeta("/test_bucket/10")
			Expect(m).ToNot(BeNil())

			p2 := proxy.NewS3Proxy(cache2)

			handler2 := http.HandlerFunc(p2.Handler)
			req, err = http.NewRequest("GET", "/test_bucket/10", nil)
			Expect(err).To(BeNil())

			rr2 := httptest.NewRecorder()
			handler2.ServeHTTP(rr2, req)

			body2, err := ioutil.ReadAll(rr2.Body)
			Expect(err).To(BeNil())
			Expect(body2).To(Equal(body1))
		})
	})
})

