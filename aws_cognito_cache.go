package aws

import (
	"log"
	"sync"
)

type Cache struct {
	mu      *sync.RWMutex
	cache   map[string]map[string]cacheData
	coginto *awsCognito
}
type cacheData struct {
	*UserType
	err error
}

func (c *Cache) init(coginto *awsCognito) {
	c.coginto = coginto
	c.cache = make(map[string]map[string]cacheData)
	c.mu = new(sync.RWMutex)
}

func (c *Cache) Get(userPoolId, sub string) (user *UserType, err error) {

	// Get user from cache using mutex read lock
	c.mu.RLock()
	userCache, ok := c.cache[userPoolId][sub]
	c.mu.RUnlock()
	if ok {
		user = userCache.UserType
		err = userCache.err
		return
	}

	// Lock cache Mutex to write
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get user from Cognito
	user, err = c.coginto.Get(userPoolId, sub)
	if err != nil {
		// Add not found user to cache
		if err.Error() == ErrCognitoUserNotFound.Error() {
			// c.cache[sub] = cacheData{user, err}
			c.add(userPoolId, sub, user, err)
			return
		}

		// Log cognito error
		log.Println("error get cognito user by sub:", err, sub)
		return
	}

	// Add user to cache
	// c.cache[sub] = cacheData{user, nil}
	c.add(userPoolId, sub, user, nil)

	return
}

func (c *Cache) add(userPoolId, sub string, user *UserType, err error) {
	if _, ok := c.cache[userPoolId]; !ok {
		c.cache[userPoolId] = make(map[string]cacheData)
	}

	c.cache[userPoolId][sub] = cacheData{user, err}
}

// Len returns the length of the cache for a given userPoolId.
func (c *Cache) Len(userPoolId string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache[userPoolId])
}

// Clear clears the cache for a given userPoolId.
func (c *Cache) Clear(userPoolId string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cache, userPoolId)
}
