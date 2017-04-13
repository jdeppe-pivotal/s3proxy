package blob_cache_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"os"
	"github.com/karlseguin/ccache"
	"s3proxy/fakes"
	"s3proxy/blob_cache"
	"golang.org/x/net/context"
	"s3proxy/context"
	"s3proxy/source"
	"time"
	"sync"
	"io"
	"fmt"
)

var _ = Describe("Testing blob cache", func() {
	Context("Sanity", func() {
		It("Just works", func() {
			cacheDir, err := ioutil.TempDir("", "cached-")
			Expect(err).To(BeNil())
			defer os.RemoveAll(cacheDir)

			bc := ccache.Layered(ccache.Configure())
			fus := fakes.NewFakeUpstreamSource(cacheDir, bc)
			cache := blob_cache.NewS3Cache(bc, fus, cacheDir, 60)

			meta := &source.Meta {
				Size: 1,
				Expires: time.Now(),
			}

			cache.AddMeta(meta, "/cached/1000000")

			wg := sync.WaitGroup{}
			wg.Add(3)

			var i uint64
			for i = 0; i < 3; i++ {
				x := i
				go func() {
					ctx := context.WithValue(context.Background(), 0, &cache_context.Context{x})
					r, _ := cache.Get(ctx, "/cached/1000000")
					var err error
					buf := make([]byte, 65536)
					var total int
					var n int
					for {
						n, err = r.Read(buf)
						total += n
						if err != nil && err == io.EOF {
							fmt.Printf("err: %s\n", err)
							break
						}
					}
					fmt.Printf("[%d] --->>> read %d\n", x, total)
					wg.Done()
				}()
			}
			wg.Wait()
		})
	})
})
