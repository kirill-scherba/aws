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

	// Get list of folders from S3
	keys, err := a.S3.List(bucket, "dust-", 0, "", "/")
	if err != nil {
		t.Log("List error:", err)
		return
	}
	if len(keys) == 0 {
		t.Log("Empty list")
		return
	}
	t.Log("folders", keys)

	// Get list of keys from folder, by 3 keys
	var list []string
	const maxKey = 3
	var lastKey string
	var prefix = keys[0] + "players/"
	t.Log("Get list of keys from folder", keys[0], "by", maxKey, "keys")
	for {
		t.Log("list after lastKey:", lastKey)

		keys, err = a.S3.List(bucket, prefix, maxKey, lastKey)
		if err != nil {
			t.Log("List error:", err)
			return
		}
		if len(keys) == 0 {
			t.Log("get list pages done")
			break
		}

		t.Log(keys)
		list = append(list, keys...)
		lastKey = keys[len(keys)-1]
	}

	// t.Log("all keys", list)
	t.Log("all keys length:", len(list))
}
