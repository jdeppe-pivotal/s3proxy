package faulting

import (
	"io"
	"sync"
	"time"
	"os"
	"path"
	"github.com/karlseguin/ccache"
	"strconv"
	"github.com/op/go-logging"
	"s3proxy/context"
	"golang.org/x/net/context"
)

const BLOCK_SIZE = 1024 * 1024

var log = logging.MustGetLogger("s3proxy")

type FaultingReader struct {
	faultingFile	*FaultingFile
	bytesRead		int64
	context         context.Context
}

func NewFaultingReader(ctx context.Context, f *FaultingFile) *FaultingReader {
	return &FaultingReader{
		faultingFile: f,
		bytesRead: 0,
		context: ctx,
	}
}

func (this *FaultingReader) Read(p []byte) (int, error) {
	if this.bytesRead >= this.Size() {
		return 0, io.EOF
	}

	// Calculate which block we need
	index := int(this.bytesRead / int64(this.faultingFile.BlockSize))
	faultedBlock, err := this.faultingFile.GetBlock(this.context, index)
	if err != nil {
		return 0, err
	}

	i := this.bytesRead - int64(index * this.faultingFile.BlockSize)
	blockEnd := int(this.faultingFile.Size - this.bytesRead + i)
	if blockEnd > this.faultingFile.BlockSize {
		blockEnd = this.faultingFile.BlockSize
	}

	n := copy(p, faultedBlock[i:blockEnd])

	ctxValue := this.context.Value(0).(*cache_context.Context)
	log.Debugf("[%d] --->>> Copied %d [%d:%d]", ctxValue.Sequence, n, i, blockEnd)

	this.bytesRead += int64(n)

	return n, nil
}

func (this *FaultingReader) Close() error {
	return nil
}

func (this *FaultingReader) Size() int64 {
	return this.faultingFile.Size
}

type FaultingFile struct {
	Src         io.Reader
	Dst         string
	BlockCache  *ccache.SecondaryCache
	BlockCount  int
	Size        int64
	UpstreamErr error
	Lock        sync.Mutex
	BlockSize   int
}

func NewFaultingFile(src io.Reader, dst string, size int64, cache *ccache.SecondaryCache) (*FaultingFile, error) {
	if _, err := os.Stat(dst); os.IsNotExist(err) {
		// Make sure we have a directory for our cached file
		err := os.MkdirAll(path.Dir(dst), 0755)
		if err != nil {
			return nil, err
		}

		f, err := os.Create(dst)
		defer f.Close()
		if err != nil {
			return nil, err
		}
	}

	return &FaultingFile{
		BlockCache: cache,
		Src: src,
		Dst: dst,
		Size: size,
		BlockSize: BLOCK_SIZE,
	}, nil
}

func (this *FaultingFile) Stream(wg *sync.WaitGroup) {
	go this.readAll(wg)
}

func (this *FaultingFile) SetBlockSize(blockSize int) {
	this.BlockSize = blockSize
}

func (this *FaultingFile) GetBlock(ctx context.Context, i int) ([]byte, error) {
	if this.UpstreamErr != nil {
		return nil, this.UpstreamErr
	}

	ctxValue := ctx.Value(0).(*cache_context.Context)
	beenWaiting := false
	for i >= this.BlockCount {
		beenWaiting = true
		log.Debugf("[%d] --->>> Waiting for block %d - only have %d", ctxValue.Sequence, i+1, this.BlockCount)
		time.Sleep(1000 * time.Millisecond)
		if this.UpstreamErr != nil {
			return nil, this.UpstreamErr
		}
	}
	if beenWaiting {
		log.Debugf("[%d] ===>>> Got block %d", ctxValue.Sequence, i+1)
	}

	entry, err := this.BlockCache.Fetch(strconv.Itoa(i), time.Second, func() (interface{}, error) {return this.faultInBlock(i)})

	if err != nil {
		return nil, err
	}

	return entry.Value().([]byte), nil
}

func (this *FaultingFile) getCachedBlock(i int) []byte {
	buf := this.BlockCache.Get(strconv.Itoa(i))
	if buf != nil {
		if byteBuf, ok := buf.Value().([]byte); ok {
			return byteBuf
		} else {
			// TODO: Should just log an error and return nil
			panic("Cache did not contain []byte")
		}
	}
	return nil
}

func (this *FaultingFile) faultInBlock(i int) ([]byte, error) {
	buf := make([]byte, this.BlockSize)

	dst, err := os.Open(this.Dst)
	if err != nil {
		return nil, err
	}
	defer dst.Close()

	sr := io.NewSectionReader(dst, int64(i * this.BlockSize), int64(this.BlockSize))

	var bytesRead int
	for bytesRead < this.BlockSize {
		n, err := sr.Read(buf[bytesRead:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		bytesRead += n
	}
	return buf, nil
}

// The WaitGroup is only used for test purposes
func (this *FaultingFile) readAll(wg *sync.WaitGroup) {
	var bytesRead int64
	var bytesWritten int64

	dstFile, err := os.Create(this.Dst)
	defer dstFile.Close()

	defer func() {
		if wg != nil {
			wg.Done()
		}
	} ()

	if err != nil {
		this.UpstreamErr = err
		return
	}

	for bytesRead < this.Size {
		buf := make([]byte, this.BlockSize)
		m, err := io.ReadFull(this.Src, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			this.UpstreamErr = err
			break
		}

		n, err := dstFile.Write(buf[:m])
		if err != nil {
			this.UpstreamErr = err
			break
		}

		bytesRead += int64(m)
		bytesWritten += int64(n)

		this.BlockCache.Set(strconv.Itoa(this.BlockCount), buf, 100)
		this.BlockCount++
	}
}
