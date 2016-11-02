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
)

var _ = Describe("S3Proxy test suite", func() {
	Context("regular proxy URLs", func() {
		It("produces the correct result", func() {
			cacheDir, err := ioutil.TempDir("", "cached-")
			Expect(err).To(BeNil())
			defer os.RemoveAll(cacheDir)

			fus := fakes.NewFakeUpstreamSource(cacheDir)
			cache := blob_cache.NewS3Cache(fus)
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
		})

		It("produces a cache file", func() {
			cacheDir, err := ioutil.TempDir("", "cached-")
			Expect(err).To(BeNil())
			defer os.RemoveAll(cacheDir)

			fus := fakes.NewFakeUpstreamSource(cacheDir)
			cache := blob_cache.NewS3Cache(fus)
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

			fus := fakes.NewFakeUpstreamSource(cacheDir)
			cache := blob_cache.NewS3Cache(fus)
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

			fus := fakes.NewFakeUpstreamSource(cacheDir)
			cache := blob_cache.NewS3Cache(fus)
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
	})
})

