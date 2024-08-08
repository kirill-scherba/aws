package aws

import (
	"testing"
)

func TestCognitoDescribeUserPool(t *testing.T) {

	_, err := New()
	if err != nil {
		t.Error(err)
		return
	}

	// Set name of your cognito user pool here
	// const cognitoUserPool = ""
	
	// num, err := a.Cognito.EstimatedNumberOfUsers(cognitoUserPool)
	// if err != nil {
	// 	t.Log(err)
	// 	return
	// }

	// t.Log("Nunber of users:", num)
}
