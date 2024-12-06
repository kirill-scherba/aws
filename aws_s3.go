package aws

import (
	"bytes"
	"context"
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// awsS3 is the AWS S3 client struct
type awsS3 struct {
	// ctx is the context.Context for AWS requests
	ctx context.Context

	// Client is the AWS S3 client
	Client *s3.Client
}

// Get return content of S3 object
func (a awsS3) Get(bucket, objectName string) (data []byte, err error) {

	// Get s3 object
	rawObject, err := a.Client.GetObject(
		a.ctx,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(objectName),
		},
	)
	if err != nil {
		return
	}

	// Read from raw object
	buf := new(bytes.Buffer)
	buf.ReadFrom(rawObject.Body)
	data = buf.Bytes()

	return
}

// Info returns metadata about an S3 object without fetching its content.
//
// Parameters:
//   - bucket: The name of the S3 bucket.
//   - objectName: The key of the S3 object.
//
// Returns:
//   - result: A pointer to the HeadObjectOutput containing metadata of the S3 object.
//   - err: An error if the operation fails.
func (a awsS3) Info(bucket, objectName string) (result *s3.HeadObjectOutput,
	err error) {

	// Prepare the HeadObjectInput with the bucket and object key
	headObj := s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectName),
	}

	// Call the HeadObject function to get the object's metadata
	return a.Client.HeadObject(a.ctx, &headObj)
}

// Set save S3 object content
func (a awsS3) Set(bucket, objectName string, data []byte) (err error) {

	buf := bytes.NewReader(data)

	_, err = a.Client.PutObject(a.ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectName),
		Body:   buf,
	})

	return
}

// Delete S3 object
func (a awsS3) Delete(bucket, objectName string) (err error) {

	_, err = a.Client.DeleteObject(a.ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectName),
	})

	return
}

// Delete S3 folder
func (a awsS3) DeleteFolder(bucket, folderName string) (err error) {

	// Check folder length and Add slash to folder name
	l := len(folderName)
	if l == 0 {
		return
	}
	if folderName[l-1] != '/' {
		folderName += "/"
	}

	// Get list objects in folder
	// TODO: MaxKeys 0 - define 1000 in aws libray by default. Make loop to 
	// delete all records 
	keys, err := a.List(bucket, folderName, 0)
	if err != nil {
		return
	}

	// Delete all objects in floder
	for i := len(keys) - 1; i >= 0; i-- {
		a.Delete(bucket, keys[i])
	}

	// Remove trailing slash in folder name
	folderName = strings.TrimRight(folderName, "/")

	// Delete folder
	a.Delete(bucket, folderName)

	return
}

// List return list of S3 objects keys in folder or returm list of prefixes if
// delimiter does not empty.
//
// Parameters:
//   - bucket - S3 bucket name
//   - prefix - limits the response to keys that begin with the specified prefix
//   - maxKeys - maximum number of keys to return (up to 1000 by default if 0)
//   - attr - aditional attributes:
//
// The attr parameter is additional and can receive the following parameters:
//   - marker - marker to use for pagination
//   - delimiter - is a character you use to group keys
//
// The marker and delimiter is not requered parameters and added to attr
// parameter. The marker first than delimiter.
//
// Marker is where you want Amazon S3 to start listing from. Amazon S3 starts
// listing after this specified key. Marker can be any key in the bucket.
//
// Returns:
//   - keys - list of S3 objects keys
//   - err - error
func (a awsS3) List(bucket, prefix string, maxKeys int, attr ...string) (
	keys []string, err error) {

	// Get attributes: marker and delimiter
	var marker, delimiter string
	if len(attr) > 0 {
		marker = attr[0]
	}
	if len(attr) > 1 {
		delimiter = attr[1]
	}

	// Get s3 object
	var maxKeysPtr *int32
	if maxKeys > 0 {
		maxKeysPtr = aws.Int32(int32(maxKeys))
	}
	listObjects, err := a.Client.ListObjects(
		a.ctx,
		&s3.ListObjectsInput{
			Bucket:    aws.String(bucket),
			Prefix:    aws.String(prefix),
			Delimiter: aws.String(delimiter),
			MaxKeys:   maxKeysPtr,
			Marker:    aws.String(marker),
		},
	)
	if err != nil {
		return
	}

	// Generate output array of prefixes
	if delimiter != "" {
		for _, p := range listObjects.CommonPrefixes {
			keys = append(keys, *p.Prefix)
		}
		return
	}

	// Generate output array of keys
	for _, obj := range listObjects.Contents {
		if *obj.Key == prefix {
			continue
		}
		keys = append(keys, *obj.Key)
	}

	return
}

// listS3 return channel with list of S3 objects keys in folder
func (a awsS3) ListChan(bucket, prefix string) (ch chan string, err error) {
	ch = make(chan string, 10)

	// Get s3 object
	listObjects, err := a.Client.ListObjects(
		a.ctx,
		&s3.ListObjectsInput{
			Bucket: aws.String(bucket),
			Prefix: aws.String(prefix),
		},
	)
	if err != nil {
		return
	}

	// Send keys to output channel
	go func() {
		for _, obj := range listObjects.Contents {
			if *obj.Key == prefix {
				continue
			}
			ch <- *obj.Key
		}
		close(ch)
	}()

	return
}

// ResponseError return aws error.
// This function check if err is aws s3.ResponseError and return it and true in
// ok. If err is not aws s3.ResponseError, this function return false in ok.
func (awsS3) ResponseError(err error) (re s3.ResponseError, ok bool) {
	if errors.As(err, &re) {
		ok = true
	}
	return
}
