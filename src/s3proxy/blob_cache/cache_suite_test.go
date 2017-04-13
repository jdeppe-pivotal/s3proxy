package blob_cache_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestBlobCache(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Blob Cache Suite")
}
