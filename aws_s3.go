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
	// TODO: MaxKeys 0 - define 1000 in aws libray by default. Make a loop to
	// delete all records
	keys, err := a.List(bucket, folderName)
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

// ListObjects is a struct that contains the parameters for the awsS3.List
// function
type ListObjects struct {

	// Sets the maximum number of keys returned in the response. By default if
	// MaxKeys equals 0, the action returns up to 1,000 key names. The response
	// might contain fewer keys but will never contain more.
	MaxKeys int

	// Marker is where you want Amazon S3 to start listing from. Amazon S3 starts
	// listing after this specified key. Marker can be any key in the bucket.
	Marker string

	// A delimiter is a character that you use to group keys.
	Delimiter string
}

// List return list of S3 objects keys in folder or returm list of prefixes if
// delimiter does not empty.
//
// Parameters:
//   - bucket - S3 bucket name
//   - prefix - limits the response to keys that begin with the specified prefix
//   - params - additional parameters:
//   - MaxKeys - maximum number of keys to return (default if 1000)
//   - Marker - marker to use for pagination
//   - Delimiter - is a character you use to group keys
//
// Marker is where you want Amazon S3 to start listing from. Amazon S3 starts
// listing after this specified key. Marker can be any key in the bucket.
//
// Returns:
//   - keys - list of S3 objects keys
//   - err - error
func (a awsS3) List(bucket, prefix string, params ...ListObjects) (
	keys []string, err error) {

	keys, _, err = a.list(bucket, prefix, params...)
	return
}

// ListTags return list of S3 objects keys and tags in folder.
//
// Parameters:
//   - bucket - S3 bucket name
//   - prefix - limits the response to keys that begin with the specified prefix
//   - params - additional parameters:
//   - MaxKeys - maximum number of keys to return (default if 1000)
//   - Marker - marker to use for pagination
//   - Delimiter - is a character you use to group keys
//
// Marker is where you want Amazon S3 to start listing from. Amazon S3 starts
// listing after this specified key. Marker can be any key in the bucket.
//
// Returns:
//   - keys - list of S3 objects keys
//   - tags - list of S3 objects tags (unique key of saves)
//   - err - error
func (a awsS3) ListTags(bucket, prefix string, params ...ListObjects) (
	keys []string, tags []string, err error) {

	return a.list(bucket, prefix, params...)
}

// list returns list of S3 objects keys in folder or returm list of prefixes if
// delimiter does not empty.
//
// Parameters:
//   - bucket - S3 bucket name
//   - prefix - limits the response to keys that begin with the specified prefix
//   - params - additional parameters:
//   - MaxKeys - maximum number of keys to return (default if 1000)
//   - Marker - marker to use for pagination
//   - Delimiter - is a character you use to group keys
//
// Marker is where you want Amazon S3 to start listing from. Amazon S3 starts
// listing after this specified key. Marker can be any key in the bucket.
//
// Returns:
//   - keys - list of S3 objects keys
//   - tags - list of S3 objects tags (unique key of saves)
//   - err - error
func (a awsS3) list(bucket, prefix string, params ...ListObjects) (
	keys []string, tags []string, err error) {

	// Set default values for parameters
	var delimiter string
	var maxKeysPtr *int32
	var markerPtr, delimiterPtr *string
	if len(params) > 0 {
		// Set maxKeys
		if params[0].MaxKeys > 0 {
			maxKeysPtr = aws.Int32(int32(params[0].MaxKeys))
		}

		// Set marker
		if params[0].Marker != "" {
			markerPtr = aws.String(params[0].Marker)
		}

		// Set delimiter
		if params[0].Delimiter != "" {
			delimiterPtr = aws.String(params[0].Delimiter)
			delimiter = params[0].Delimiter
		}
	}

	// Get list of S3 objects
	listObjects, err := a.Client.ListObjects(
		a.ctx,
		&s3.ListObjectsInput{
			Bucket:    aws.String(bucket),
			Prefix:    aws.String(prefix),
			Delimiter: delimiterPtr,
			MaxKeys:   maxKeysPtr,
			Marker:    markerPtr,
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

	// Generate output array of times
	for _, obj := range listObjects.Contents {
		if *obj.Key == prefix {
			continue
		}
		// t, _ := time.Parse(time.RFC3339, *obj.LastModified)
		tag := ""
		if obj.ETag != nil {
			tag = *obj.ETag
		}
		tags = append(tags, tag)
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
