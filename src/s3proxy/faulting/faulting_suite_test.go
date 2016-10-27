package faulting_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestFaulting(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Faulting Suite")
}
