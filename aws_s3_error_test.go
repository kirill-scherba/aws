package aws

import (
	"fmt"
	"testing"
)

// TestGetS3 checks aws error when get S3 data
func TestGetS3Error(t *testing.T) {

	a, err := New()
	if err != nil {
		t.Error(err)
		return
	}

	// Test AwsError function
	err = fmt.Errorf("an error")
	_, ok := a.AwsError(err)
	if !ok {
		t.Logf("the '%s' is not aws error", err.Error())
	}

	// Try get not existing S3 key and check aws error
	data, err := a.S3.Get(bucket, "not_existing_key")
	if err != nil {

		t.Log(err)

		// Get aws s3 error
		resErr, _ := a.S3.ResponseError(err)
		t.Logf("\nError: %s\nServiceHostID: %s\nServiceRequestID: %s\n", resErr.Error(), resErr.ServiceHostID(), resErr.ServiceRequestID())

		// Get aws common error
		awsErr, _ := a.AwsError(err)
		t.Logf("\nErrorCode: %s, ErrorMessage: %s, ErrorFault: %d", awsErr.ErrorCode(), awsErr.ErrorMessage(), awsErr.ErrorFault())

		// The awsErr.ErrorCode() return responsible error code

		return
	}
	t.Log(len(data))
}
