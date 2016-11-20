package source

import (
	"s3proxy/faulting"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"strings"
	"path"
	"github.com/karlseguin/ccache"
	"github.com/op/go-logging"
)

type S3Source struct{
	session      *session.Session
	blockCache   *ccache.LayeredCache
	baseCacheDir string
}

var log = logging.MustGetLogger("s3proxy")

func NewS3Source(cache *ccache.LayeredCache, region, cacheDir string) *S3Source {
	// The session the S3 Downloader will use
	sess := session.New(&aws.Config{
		Region: aws.String(region),
	})

	return &S3Source{
		session: sess,
		blockCache: cache,
		baseCacheDir: cacheDir,
	}
}

func (this S3Source) Get(uri string) (*faulting.FaultingFile, *Meta, error) {

	bucket, object := splitS3Uri(uri)

	svc := s3.New(this.session)

	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}

	getResp, err := svc.GetObject(params)
	if err != nil {
		return nil, nil, err
	}

	// TODO: Write a Reader which implements ReadAt but assumes the offset will always be increasing
	//pReader, pWriter := io.Pipe()
	//// Create a downloader with the session and default options
	//downloader := s3manager.NewDownloader(this.session)
	//go downloader.Download(pWriter, params)

	objectFile := path.Join(this.baseCacheDir, bucket, object)
	sCache := this.blockCache.GetOrCreateSecondaryCache(uri)
	ff, err := faulting.NewFaultingFile(getResp.Body, objectFile, *getResp.ContentLength, sCache)
	if err != nil {
		return nil, nil, err
	}

	ff.Stream(nil)
	meta := &Meta{
		Size: *getResp.ContentLength,
		LastModified: *getResp.LastModified,
		ContentType: *getResp.ContentType,
		ETag: *getResp.ETag,
	}

	return ff, meta, nil
}

func (this S3Source) GetMeta(uri string) (*Meta, error) {
	bucket, object := splitS3Uri(uri)
	svc := s3.New(this.session)

	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}

	headResp, err := svc.HeadObject(params)
	if err != nil {
		return nil, err
	}

	return &Meta{
		Size: *headResp.ContentLength,
		LastModified: *headResp.LastModified,
		ContentType: *headResp.ContentType,
		ETag: *headResp.ETag,
	}, nil
}

func splitS3Uri(uri string) (string, string) {
	uri = strings.TrimLeft(uri, "/")
	idx := strings.Index(uri, "/")

	if idx < 0 {
		return uri, ""
	}

	if idx + 1 == len(uri) {
		return uri, ""
	}

	return uri[:idx], uri[idx+1:]
}
