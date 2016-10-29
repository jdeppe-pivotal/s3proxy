package source

import (
	"s3proxy/faulting"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"strings"
	"path"
)

type S3Source struct{
	session    *session.Session
}

func NewS3Source() *S3Source {
	// The session the S3 Downloader will use
	session := session.New(&aws.Config{
		Region: aws.String("us-west-2"),
	})

	return &S3Source{
		session: session,
	}
}

func (this S3Source) Get(uri string) (*faulting.FaultingFile, *Meta, error) {

	bucket, object := splitS3Uri(uri)

	svc := s3.New(this.session)

	headParams := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}

	headResp, err := svc.HeadObject(headParams)
	if err != nil {
		return nil, nil, err
	}

	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}

	getResp, err := svc.GetObject(params)

	// TODO: Write a Reader which implements ReadAt but assumes the offset will always be increasing
	//pReader, pWriter := io.Pipe()
	//// Create a downloader with the session and default options
	//downloader := s3manager.NewDownloader(this.session)
	//go downloader.Download(pWriter, params)

	bucketObject := path.Join(bucket, object)
	ff, err := faulting.NewFaultingFile(getResp.Body, bucketObject, *headResp.ContentLength)
	if err != nil {
		return nil, nil, err
	}

	ff.Stream(nil)
	meta := &Meta{
		Size: *headResp.ContentLength,
		LastModified: *headResp.LastModified,
		ContentType: *headResp.ContentType,
	}

	return ff, meta, nil
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
