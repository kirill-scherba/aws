// Copyright 2022-2023 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Helper golang package to easy execute Lambda and S3 AWS SDK functions
package aws

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Aws methods receiver and data structure
type Aws struct {
	S3     awsS3
	Lambda awsLambda
}

type awsS3 struct {
	ctx context.Context
	*s3.Client
}
type awsLambda struct {
	ctx context.Context
	*lambda.Client
}

// New creates AWS S3 and Lambda clients
func New(region ...string) (a *Aws, err error) {

	a = new(Aws)

	// Load AWS config
	ctx := context.TODO()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		err = fmt.Errorf("lambda configuration error, %s", err)
		return
	}

	// Set connections region
	if len(region) > 0 {
		cfg.Region = region[0]
	}

	// Create new Lambda client
	a.Lambda.ctx = ctx
	a.Lambda.Client = lambda.NewFromConfig(cfg)

	// Create new S3 client
	a.S3.ctx = ctx
	a.S3.Client = s3.NewFromConfig(cfg)

	return
}

// Get execute Lambda function and return result
func (a awsLambda) Get(funcName string, request any) (result *lambda.InvokeOutput, err error) {
	payload, err := json.Marshal(request)
	if err != nil {
		err = fmt.Errorf("can't marshal lambda %s request, error %s",
			funcName, err)
		return
	}

	// Execute lambda
	result, err = a.Client.Invoke(a.ctx, &lambda.InvokeInput{
		FunctionName: aws.String(funcName),
		Payload:      payload},
	)
	if err != nil {
		err = fmt.Errorf("error calling lambda %s: %s", funcName, err)
	}

	return
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
		err = fmt.Errorf("got an error getting Object %s, error: %s",
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

// List return list of S3 objects keys in folder
func (a awsS3) List(bucket, prefix string) (keys []string, err error) {

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
			"got an error getting Objects list from bucket: %s, error: %s",
			prefix, err,
		)
		return
	}

	// Generate output array
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
			"got an error getting Objects list from bucket: %s, error: %s",
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
