// To execute this test set POOL environment variable with name of your cognito
// user pool id and SUB of existing user:
//
//   POOL=XXX SUB=UUU go test -v -count=1 .

package aws

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// var cognitoUserPool string
//
// func init() {
// 	cognitoUserPool = os.Getenv("POOL")
// }

func TestCognitoCache(t *testing.T) {

	if cognitoUserPool == "" {
		t.Skip()
		return
	}

	// Create aws client
	a, err := New()
	if err != nil {
		t.Error(err)
		return
	}

	// Get from cognito cache user by sub.
	//   if not found get from cognito and add to cache
	//   if found get from cache

	// Get not existing user "sub"
	_, err = a.Cognito.Cache.Get(cognitoUserPool, "sub")
	if err != nil {
		fmt.Println("get not existing user error:", err)
	}
	if err == nil {
		t.Error("get does not return error")
		return
	}
	if err != ErrCognitoUserNotFound {
		t.Error("get return wrong error:", err)
		return
	}

	// Get not existing user "sub" from cache. The "sub" with error was added to
	// cache after previous Cache.Get
	_, err = a.Cognito.Cache.Get(cognitoUserPool, "sub")
	if err != nil {
		fmt.Println("get not existing user from cache error:", err)
	}
	if err == nil {
		t.Error("get from cache does not return error")
		return
	}
	if err != ErrCognitoUserNotFound {
		t.Error("get from cache return wrong error:", err)
		return
	}

	sub := os.Getenv("SUB")
	if sub == "" {
		t.Skip()
		return
	}

	// Get existing user sub
	start := time.Now()
	user, err := a.Cognito.Cache.Get(cognitoUserPool, sub)
	if err != nil {
		t.Error(err)
		return
	}
	if user == nil {
		t.Error("get return nil user")
		return
	}
	fmt.Printf("get user by sub: %s, %s\n", sub, time.Since(start))

	// Get existing user sub from cache. The "sub" with error was added to
	// cache after previous Cache.Get
	start = time.Now()
	user, err = a.Cognito.Cache.Get(cognitoUserPool, sub)
	if err != nil {
		t.Error(err)
		return
	}
	if user == nil {
		t.Error("get return nil user")
		return
	}
	fmt.Printf("get user by sub from cache: %s, %s\n", sub, time.Since(start))

	// Clear cache
	a.Cognito.Cache.Clear(cognitoUserPool)
	start = time.Now()
	user, err = a.Cognito.Cache.Get(cognitoUserPool, sub)
	if err != nil {
		t.Error(err)
		return
	}
	if user == nil {
		t.Error("get return nil user")
		return
	}
	fmt.Printf("get user by sub after clear cache: %s, %s\n", sub, time.Since(start))
}
