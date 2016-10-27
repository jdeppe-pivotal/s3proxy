package proxy_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestS3proxy(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "S3proxy Suite")
}
