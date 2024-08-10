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
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Aws methods receiver and data structure
type Aws struct {
	S3      awsS3
	Lambda  awsLambda
	Cognito awsCognito
}

type awsS3 struct {
	ctx context.Context
	*s3.Client
}
type awsLambda struct {
	ctx context.Context
	*lambda.Client
}
type awsCognito struct {
	ctx context.Context
	*cognitoidentityprovider.Client
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
			"got an error getting s3 Objects list from bucket: %s, error: %s",
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

// Get retrieves a Cognito UserType by its user pool ID and sub.
//
// Parameters:
// - userPoolId: The ID of the user pool.
// - sub: Users sub id.
//
// Returns:
// - user: A pointer to the retrieved UserType.
// - err: An error if the operation fails.
func (a awsCognito) Get(userPoolId, sub string) (user *types.UserType, err error) {
	// Call the ListUsers API to retrieve the user from the user pool.
	listUsers, err := a.Client.ListUsers(a.ctx, &cognitoidentityprovider.ListUsersInput{
		UserPoolId: aws.String(userPoolId),           // Set the user pool ID.
		Filter:     aws.String("sub^='" + sub + "'"), // Filter users by sub.
	})
	if err != nil {
		return // Return the error if there was an issue calling the API.
	}

	// Check if no users were found.
	if len(listUsers.Users) == 0 {
		err = errors.New("not found") // Return an error if no user was found.
		return
	}

	// Return the first user in the list.
	user = &listUsers.Users[0]

	return
}

// Length retrieves the estimated number of users in a user pool.
//
// Parameters:
// - userPoolId: The ID of the user pool.
//
// Returns:
// - estimatedNumberOfUsers: The estimated number of users in the user pool.
// - err: An error if the operation fails.
func (a awsCognito) Length(userPoolId string) (
	estimatedNumberOfUsers int, err error) {

	// Call the DescribeUserPool API to get the user pool details.
	out, err := a.DescribeUserPool(a.ctx,
		&cognitoidentityprovider.DescribeUserPoolInput{
			UserPoolId: aws.String(userPoolId),
		},
	)
	if err != nil {
		return
	}

	// Extract the estimated number of users from the response and return it.
	estimatedNumberOfUsers = int(out.UserPool.EstimatedNumberOfUsers)
	return
}

// A user profile in a Amazon Cognito user pool.
type UserType = types.UserType

// List retrieves a list of Cognito users from a user pool.
//
// Parameters:
//
// userPoolId: The ID of the user pool.
//
// limit: The maximum number of users to return.
//
// filter: A filter string to limit the users returned.
// Quotation marks within the filter string must be escaped using the backslash (\)
// character. For example, " family_name = \"Reddy\"":
//
//   - AttributeName: The name of the attribute to search for. You can only search
//     for one attribute at a time.
//   - Filter-Type: For an exact match, use =, for example, " given_name =
//     \"Jon\"". For a prefix ("starts with") match, use ^=, for example, " given_name
//     ^= \"Jon\"".
//   - AttributeValue: The attribute value that must be matched for each user.
//
// If the filter string is empty, ListUsers returns all users in the user pool.
// You can only search for the following standard attributes:
//   - username (case-sensitive)
//   - email
//   - phone_number
//   - name
//   - given_name
//   - family_name
//   - preferred_username
//   - cognito:user_status (called Status in the Console) (case-insensitive)
//   - status (called Enabled in the Console) (case-sensitive)
//   - sub
//
// previous: An identifier that was returned from the previous call to this
// operation, which can be used to return the next set of items in the list.
//
// Returns:
//
//   - users: A list of UserType objects representing the users.
//   - pagination: A token to continue the list from if there are more users.
//   - err: An error if the operation fails.
func (a awsCognito) List(userPoolId string, limit int, filter string, previous *string) (
	users []UserType, pagination *string, err error) {
	// Call the ListUsers API to retrieve the user from the user pool.

	// Set the user pool ID.
	input := &cognitoidentityprovider.ListUsersInput{
		UserPoolId:      aws.String(userPoolId),
		Limit:           aws.Int32(int32(limit)),
		Filter:          aws.String(filter),
		PaginationToken: previous,
	}

	// Call the ListUsers API to retrieve the user from the user pool.
	listUsers, err := a.Client.ListUsers(a.ctx, input)
	if err != nil {
		return // Return the error if there was an issue calling the API.
	}

	// Return the list of users and the pagination token.
	pagination = listUsers.PaginationToken
	users = listUsers.Users
	return
}
