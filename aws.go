// Copyright 2022-2023 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Helper golang package to easy execute Lambda, S3 and Cognito AWS SDK functions.
package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Aws methods receiver and data structure
type Aws struct {
	S3      awsS3
	Lambda  awsLambda
	Cognito awsCognito
}

// New creates AWS S3 and Lambda clients
func New(region ...string) (a *Aws, err error) {

	a = new(Aws)

	// Load AWS config
	ctx := context.TODO()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		err = fmt.Errorf("aws configuration error, %s", err)
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

	// Create new Cognito client
	a.Cognito.ctx = ctx
	a.Cognito.Client = cognitoidentityprovider.NewFromConfig(cfg)

	return
}
