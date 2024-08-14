package aws

import (
	"bytes"
	"context"
	"fmt"
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
		err = fmt.Errorf("got an error getting s3 Object %s, error: %s",
			objectName, err)
		return
	}

	// Read from raw object
	buf := new(bytes.Buffer)
	buf.ReadFrom(rawObject.Body)
	data = buf.Bytes()

	return
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
	keys, err := a.List(bucket, folderName, "", 0, "")
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
//   - maxKeys - maximum number of keys to return (up to 1000 by default if is 0)
//   - marker - marker to use for pagination
//   - delimiter - is a character you use to group keys
//
// Marker is where you want Amazon S3 to start listing from. Amazon S3 starts
// listing after this specified key. Marker can be any key in the bucket.
//
// Returns:
//   - keys - list of S3 objects keys
//   - err - error
func (a awsS3) List(bucket, prefix, delimiter string, maxKeys int, marker string) (keys []string, err error) {

	// Get s3 object
	listObjects, err := a.Client.ListObjects(
		a.ctx,
		&s3.ListObjectsInput{
			Bucket:    aws.String(bucket),
			Prefix:    aws.String(prefix),
			Delimiter: aws.String(delimiter),
			MaxKeys:   int32(maxKeys),
			Marker:    aws.String(marker),
		},
	)
	if err != nil {
		err = fmt.Errorf(
			"got an error getting s3 Objects list from bucket: %s, error: %s",
			prefix, err,
		)
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
		err = fmt.Errorf(
			"got an error getting s3 Objects list from bucket: %s, error: %s",
			prefix, err,
		)
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
