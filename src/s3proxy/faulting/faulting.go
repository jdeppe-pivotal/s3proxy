package faulting

import (
	"io"
	"sync"
	"time"
	"os"
	"path"
	"github.com/karlseguin/ccache"
	"strconv"
)

const BLOCK_SIZE = 1024 * 1024

type FaultingReader struct {
	faultingFile	*FaultingFile
	bytesRead		int64
}

func NewFaultingReader(f *FaultingFile) *FaultingReader {
	return &FaultingReader{
		faultingFile: f,
		bytesRead: 0,
	}
}

func (this *FaultingReader) Read(p []byte) (int, error) {
	if this.bytesRead >= this.faultingFile.Size {
		return 0, io.EOF
	}

	// Calculate which block we need
	index := int(this.bytesRead / int64(this.faultingFile.blockSize))
	faultedBlock, err := this.faultingFile.GetBlock(index)
	if err != nil {
		return 0, err
	}

	i := this.bytesRead - int64(index * this.faultingFile.blockSize)
	blockEnd := int(this.faultingFile.Size - this.bytesRead + i)
	if blockEnd > this.faultingFile.blockSize {
		blockEnd = this.faultingFile.blockSize
	}

	n := copy(p, faultedBlock[i:blockEnd])
	//fmt.Printf("--->> Reading block: %d total: %d n: %d\n", index, this.bytesRead, n)

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
	src				io.Reader
	dst             string
	dstFile         *os.File
	blocks			*ccache.Cache
	blockCount      int
	Size			int64
	UpstreamErr		error
	lock            sync.Mutex
	blockSize		int
}

func NewFaultingFile(src io.Reader, dst string, size int64) (*FaultingFile, error) {
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

	cache := ccache.New(ccache.Configure().MaxSize(1000).ItemsToPrune(100))

	return &FaultingFile{
		blocks: cache,
		src: src,
		dst: dst,
		Size: size,
		blockSize: BLOCK_SIZE,
	}, nil
}

func (this *FaultingFile) Stream(wg *sync.WaitGroup) {
	go this.readAll(wg)
}

func (this *FaultingFile) SetBlockSize(blockSize int) {
	this.blockSize = blockSize
}

func (this *FaultingFile) GetBlock(i int) ([]byte, error) {
	if this.UpstreamErr != nil {
		return nil, this.UpstreamErr
	}

	for i >= this.blockCount {
		time.Sleep(100 * time.Millisecond)
		if this.UpstreamErr != nil {
			return nil, this.UpstreamErr
		}
	}

	buf, err := this.blocks.Fetch(strconv.Itoa(i), time.Second, func() (interface{}, error) {return this.faultInBlock(i)})

	if err != nil {
		return nil, err
	}

	return buf.Value().([]byte), nil
}

func (this *FaultingFile) getCachedBlock(i int) []byte {
	buf := this.blocks.Get(strconv.Itoa(i))
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
	buf := make([]byte, this.blockSize)

	dst, err := os.Open(this.dst)
	if err != nil {
		return nil, err
	}
	defer dst.Close()

	sr := io.NewSectionReader(dst, int64(i * this.blockSize), int64(this.blockSize))

	var bytesRead int
	for bytesRead < this.blockSize {
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

func (this *FaultingFile) readAll(wg *sync.WaitGroup) {
	var bytesRead int64
	var bytesWritten int64

	dstFile, err := os.Create(this.dst)
	defer dstFile.Close()
	//defer this.src.Close()

	if err != nil {
		this.UpstreamErr = err
		if wg != nil {
			wg.Done()
		}
		return
	}

	for bytesRead < this.Size {
		buf := make([]byte, this.blockSize)
		m, err := io.ReadFull(this.src, buf)
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

		this.blocks.Set(strconv.Itoa(this.blockCount), buf, 100)
		this.blockCount++
	}
	if wg != nil {
		wg.Done()
	}
}
