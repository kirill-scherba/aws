// Copyright 2022-2023 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Helper golang package to easy execute Lambda and S3 AWS SDK functions
package aws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

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

// NewAws creates AWS S3 and Lambda clients
func NewAws() (a *Aws, err error) {

	a = new(Aws)

	// Load AWS config
	ctx := context.TODO()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		err = errors.New("lambda configuration error, " + err.Error())
		return
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
		err = errors.New("error marshalling lambda " + funcName + " request")
		return
	}

	// Execute lambda
	result, err = a.Client.Invoke(a.ctx, &lambda.InvokeInput{
		FunctionName: aws.String(funcName),
		Payload:      payload},
	)
	if err != nil {
		err = errors.New("error calling lambda " + funcName + ": " + err.Error())
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
		fmt.Println("Got an error getting Object " + objectName)
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

	a.Client.PutObject(a.ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectName),
		Body:   buf,
	})

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
		fmt.Println("Got an error getting Objects list from bucket: " + prefix)
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
		fmt.Println("Got an error getting Objects list from bucket: " + prefix)
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
