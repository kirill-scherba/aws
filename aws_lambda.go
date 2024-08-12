package aws

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
)

// awsLambda is a struct that represents AWS Lambda client.
// It contains context and AWS Lambda client.
type awsLambda struct {
	// context represents the context in which the Lambda function is executed
	ctx context.Context

	// Client represents the client for the AWS Lambda service
	Client *lambda.Client
}

// Get executes the AWS Lambda function with the given function name and request.
// It returns the result of the function execution and an error if any.
//
// Parameters:
// - funcName: The name of the AWS Lambda function to be executed.
// - request: The request payload for the AWS Lambda function.
//
// Returns:
// - result: The output of the AWS Lambda function execution.
// - err: An error if the function execution fails.
func (a awsLambda) Get(funcName string, request any) (
	result *lambda.InvokeOutput, err error) {

	// Marshal the request payload into JSON format
	payload, err := json.Marshal(request)
	if err != nil {
		// Return an error if the request payload cannot be marshaled
		err = fmt.Errorf("can't marshal lambda %s request, error %s",
			funcName, err)
		return
	}

	// Execute the AWS Lambda function
	result, err = a.Client.Invoke(a.ctx, &lambda.InvokeInput{
		FunctionName: aws.String(funcName), // Set the function name
		Payload:      payload,              // Set the payload
	})
	if err != nil {
		// Return an error if the function execution fails
		err = fmt.Errorf("error calling lambda %s: %s", funcName, err)
	}

	return
}
