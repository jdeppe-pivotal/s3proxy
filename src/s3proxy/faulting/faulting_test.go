package faulting_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io"
	"sync"
	"io/ioutil"
	"os"
	"s3proxy/faulting"
	"s3proxy/fakes"
	"github.com/jdeppe-pivotal/ccache"
)

var _ = Describe("When faulting in a file", func() {
	Context("StreamingSource works", func() {
		It("is created correctly", func() {
			ss := fakes.NewIntegerStreamingSource(10)
			Expect(string(ss.Content)).To(Equal("0 1 2 3 4 5 6 7 8 9 "))
		})

		It("can be read correctly", func() {
			ss := fakes.NewIntegerStreamingSource(10)
			buf := make([]byte, 2)
			n, err := ss.Read(buf)

			Expect(err).To(BeNil())
			Expect(n).To(Equal(2))
			Expect(string(buf)).To(Equal("0 "))

			ss.Read(buf)
			Expect(string(buf)).To(Equal("1 "))

			buf = make([]byte, 100)
			n, _ = ss.Read(buf)
			Expect(string(buf[:n])).To(Equal("2 3 4 5 6 7 8 9 "))

			n, err = ss.Read(buf)
			Expect(n).To(Equal(0))
			Expect(err).ToNot(BeNil())
		})

		It("wIll return EOF", func() {
			ss := fakes.NewIntegerStreamingSource(10)
			buf := make([]byte, 100)
			n, err := ss.Read(buf)

			Expect(err).To(BeNil())
			Expect(string(buf[:n])).To(Equal("0 1 2 3 4 5 6 7 8 9 "))

			n, err = ss.Read(buf)
			Expect(n).To(Equal(0))
			Expect(err).To(Equal(io.EOF))
		})
	})

	Context("sanity", func() {
		It("more sanity", func() {
			ss := fakes.NewIntegerStreamingSource(1000)
			cacheFile, err := ioutil.TempFile("", "cache-")
			Expect(err).To(BeNil())
			defer os.Remove(cacheFile.Name())
			cacheFile.Close()

			cache := ccache.Layered(ccache.Configure().MaxSize(100))
			sCache := cache.GetOrCreateSecondaryCache("primary")
			ff, err := faulting.NewFaultingFile(ss, cacheFile.Name(), int64(len(ss.Content)), sCache)
			Expect(err).To(BeNil())

			ff.SetBlockSize(3)

			var wg sync.WaitGroup
			wg.Add(1)
			ff.Stream(&wg)
			wg.Wait()

			Expect(ff.UpstreamErr).To(BeNil())

			sinkData, err := ioutil.ReadFile(cacheFile.Name())
			Expect(err).To(BeNil())
			Expect(sinkData).To(Equal(ss.Content))
		})

		//It("has correct number of backing blocks", func() {
		//	ss := fakes.NewIntegerStreamingSource(1000)
		//	cacheFile, err := ioutil.TempFile("", "cached2")
		//	Expect(err).To(BeNil())
		//	defer os.Remove(cacheFile.Name())
		//	cacheFile.Close()
		//
		//	ff, err := faulting.NewFaultingFile(ss, cacheFile.Name(), int64(len(ss.Content)))
		//	Expect(err).To(BeNil())
		//
		//	ff.SetBlockSize(11)
		//
		//	var wg sync.WaitGroup
		//	wg.Add(1)
		//	ff.Stream(&wg)
		//	wg.Wait()
		//
		//	Expect(ff.UpstreamErr).To(BeNil())
		//
		//	blockCount := len(ff.GetBlocks())
		//	Expect(blockCount).To(Equal(354))
		//
		//	// Check the last block
		//	lastLen := len(ss.Content) - (353 * 11)
		//	Expect(string(ff.GetBlocks()[blockCount-1][:lastLen])).To(Equal("98 999 "))
		//})

		It("FaultingReader wraps FaultingFile", func() {
			ss := fakes.NewIntegerStreamingSource(1000)
			cacheFile, err := ioutil.TempFile("", "cached3")
			Expect(err).To(BeNil())
			defer os.Remove(cacheFile.Name())
			cacheFile.Close()

			cache := ccache.Layered(ccache.Configure().MaxSize(100))
			sCache := cache.GetOrCreateSecondaryCache("primary")
			ff, err := faulting.NewFaultingFile(ss, cacheFile.Name(), int64(len(ss.Content)), sCache)
			Expect(err).To(BeNil())

			ff.SetBlockSize(11)

			fr := faulting.NewFaultingReader(ff)
			buf := make([]byte, 10)

			var wg sync.WaitGroup
			wg.Add(1)
			ff.Stream(&wg)
			wg.Wait()

			Expect(ff.UpstreamErr).To(BeNil())

			n, err := fr.Read(buf)
			Expect(err).To(BeNil())
			Expect(n).To(Equal(10))
		})

		It("FaultingReader with different buffer sizes to Read", func() {
			ss := fakes.NewIntegerStreamingSource(1000)
			cacheFile, err := ioutil.TempFile("", "cached3")
			Expect(err).To(BeNil())
			defer os.Remove(cacheFile.Name())
			cacheFile.Close()

			cache := ccache.Layered(ccache.Configure().MaxSize(100))
			sCache := cache.GetOrCreateSecondaryCache("primary")
			ff, err := faulting.NewFaultingFile(ss, cacheFile.Name(), int64(len(ss.Content)), sCache)
			Expect(err).To(BeNil())

			ff.SetBlockSize(11)
			fr := faulting.NewFaultingReader(ff)

			var wg sync.WaitGroup
			wg.Add(1)
			ff.Stream(&wg)
			wg.Wait()

			Expect(ff.UpstreamErr).To(BeNil())

			buf1 := make([]byte, 10)
			n, err := fr.Read(buf1)
			Expect(err).To(BeNil())
			Expect(string(buf1[:n])).To(Equal("0 1 2 3 4 "))

			buf2 := make([]byte, 20)
			n, err = fr.Read(buf2)
			Expect(err).To(BeNil())
			Expect(string(buf2[:n])).To(Equal("5"))

			n, err = fr.Read(buf2)
			Expect(err).To(BeNil())
			Expect(string(buf2[:n])).To(Equal(" 6 7 8 9 10"))
		})

		It("FaultingReader with default block size", func() {
			ss := fakes.NewIntegerStreamingSource(1000)
			cacheFile, err := ioutil.TempFile("", "cached3")
			Expect(err).To(BeNil())
			defer os.Remove(cacheFile.Name())
			cacheFile.Close()

			cache := ccache.Layered(ccache.Configure().MaxSize(100))
			sCache := cache.GetOrCreateSecondaryCache("primary")
			ff, err := faulting.NewFaultingFile(ss, cacheFile.Name(), int64(len(ss.Content)), sCache)
			Expect(err).To(BeNil())

			fr := faulting.NewFaultingReader(ff)

			var wg sync.WaitGroup
			wg.Add(1)
			ff.Stream(&wg)
			wg.Wait()

			Expect(ff.UpstreamErr).To(BeNil())

			buf1 := make([]byte, 10)
			n, err := fr.Read(buf1)

			Expect(err).To(BeNil())
			Expect(string(buf1[:n])).To(Equal("0 1 2 3 4 "))

			buf2 := make([]byte, 20)
			n, err = fr.Read(buf2)
			Expect(err).To(BeNil())
			Expect(string(buf2[:n])).To(Equal("5 6 7 8 9 10 11 12 1"))

			n, err = fr.Read(buf2)
			Expect(err).To(BeNil())
			Expect(string(buf2[:n])).To(Equal("3 14 15 16 17 18 19 "))
		})
	})
})

