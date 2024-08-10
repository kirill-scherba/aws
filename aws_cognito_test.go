// To execute this test set POOL environment variable with name of your cognito 
// user pool id:
//
//   POOL=XXX go test -v -count=1 .

package aws

import (
	"os"
	"testing"
)

var cognitoUserPool string

func init() {
	cognitoUserPool = os.Getenv("POOL")
}

func TestCognitoDescribeUserPool(t *testing.T) {

	if cognitoUserPool == "" {
		t.Skip()
		return
	}

	a, err := New()
	if err != nil {
		t.Error(err)
		return
	}

	num, err := a.Cognito.Length(cognitoUserPool)
	if err != nil {
		t.Log(err)
		return
	}

	t.Log("Nunber of users:", num)
}

func TestCognitoListUsers(t *testing.T) {

	if cognitoUserPool == "" {
		t.Skip()
		return
	}

	a, err := New()
	if err != nil {
		t.Error(err)
		return
	}

	// Set name of your cognito user pool here
	var previous *string
	for i := 0; i < 10; i++ {
		users, p, err := a.Cognito.List(cognitoUserPool, 10, "", previous)
		if err != nil {
			t.Log(err)
			return
		}
		previous = p
		t.Log("Users list length:", len(users))
		for _, user := range users {
			t.Log(*user.Username)
		}
		t.Log()
	}
}
