package aws

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
)

type awsCognito struct {
	ctx context.Context
	*cognitoidentityprovider.Client
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

// UserAttributes returns a map of user attributes.
//
// Parameters:
//
// - user: Pointer to the UserType struct representing the user.
//
// Returns:
//
// - m: A map of string keys and string values representing the user attributes.
func (awsCognito) UserAttributes(user *UserType) (m map[string]string) {
	// Create a new map to store the user attributes.
	m = make(map[string]string)

	// Iterate over each attribute of the user.
	for _, attribute := range user.Attributes {
		// Convert the attribute name and value to strings and store them in the map.
		m[aws.ToString(attribute.Name)] = aws.ToString(attribute.Value)
	}

	// Return the map of user attributes.
	return
}
