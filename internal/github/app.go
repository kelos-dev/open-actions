package github

import (
	"context"
	"crypto/sha256"
	"time"
)

const appCacheTTL = 5 * time.Minute

type appCacheKey struct {
	AppID          int64
	PrivateKeyHash [sha256.Size]byte
}

type cachedApp struct {
	login     string
	expiresAt time.Time
}

type appRequest struct {
	done  chan struct{}
	login string
	err   error
}

// AppBotLogin returns the bot login associated with a GitHub App. Successful
// lookups are cached for five minutes per App and private key.
func (c *Client) AppBotLogin(ctx context.Context, appID int64, privateKey []byte) (string, error) {
	key := appCacheKey{AppID: appID, PrivateKeyHash: sha256.Sum256(privateKey)}
	now := c.now()
	c.appMutex.Lock()
	for key, cached := range c.appCache {
		if !cached.expiresAt.After(now) {
			delete(c.appCache, key)
		}
	}
	if cached, found := c.appCache[key]; found {
		c.appMutex.Unlock()
		return cached.login, nil
	}
	if pending, found := c.appPending[key]; found {
		c.appMutex.Unlock()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-pending.done:
			return pending.login, pending.err
		}
	}
	pending := &appRequest{done: make(chan struct{})}
	c.appPending[key] = pending
	c.appMutex.Unlock()

	login, err := c.appBotLogin(ctx, appID, privateKey)
	c.appMutex.Lock()
	pending.login, pending.err = login, err
	delete(c.appPending, key)
	if err == nil {
		c.appCache[key] = cachedApp{login: login, expiresAt: c.now().Add(appCacheTTL)}
	}
	close(pending.done)
	c.appMutex.Unlock()
	return login, err
}
