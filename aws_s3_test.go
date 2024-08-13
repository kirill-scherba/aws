package aws

import (
	"os"
	"testing"
)

var bucket string

func init() {
	bucket = os.Getenv("BUCKET")
}

func TestS3List(t *testing.T) {

	a, err := New()
	if err != nil {
		t.Error(err)
		return

	}

	// Get list of prefixes from S3
	keys, err := a.S3.List(bucket, "dust-", "/")
	if err != nil {
		return

	}

	t.Log(keys)
	t.Log(len(keys))
}
