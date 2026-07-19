package src

import (
	"sync"
	"time"
)

type UserManager struct {
	Users map[string]*User
	Mu    *sync.RWMutex
}

type User struct {
	Id               string
	Mu               sync.RWMutex
	Sessions         map[string]*Session
	CurrentSessionId string
	SharedCache      *Cache[string, any]
	memory           *memorylimit
	pool             *activeSessionsRegistry
	// CurrentSessionPool uint8
	isActive bool
}

type userSnapShot struct {
	id               string
	currentSessionId string
	isActive         bool
	remainingSpace   uint64
	sessions         []string
	currentSessions  uint8
}

type Session struct {
	SessionId     string
	sessionToken  string
	refreshToken  string
	isActive      bool
	sessionExpiry time.Time
	refreshExpiry time.Time
	lastAccessed  time.Time
	cache         *Cache[string, any]
	mu            sync.RWMutex
}

type sessionSnapshot struct {
	sessionId    string
	sessionToken string
	refreshToken string
	// isActive      bool
	sessionExpiry time.Time
	refreshExpiry time.Time
	err           error
}

type cacheItem[T any] struct {
	Value        T
	ExpiryTime   time.Time
	LastAccessed time.Time
}

type Cache[K comparable, V any] struct {
	Mu    sync.RWMutex
	Store map[K]cacheItem[V]
}

func newCache[K comparable, V any]() *Cache[K, V] {
	return &Cache[K, V]{
		Store: make(map[K]cacheItem[V]),
	}
}

func newCacheValue[K comparable, V any](userid string, value any) *Cache[K, V] {
	return &Cache[K, V]{
		Store: make(map[K]cacheItem[V]),
	}
}

// type userPayload struct {
// 	id    string
// 	key   string
// 	value any
// }

type userDTO struct {
	user              userSnapShot
	isNew             bool
	sessionTokenToAdd string
	pool              *activeSessionsRegistry
}

type sessionPoolConfigDTO struct {
	pool  *activeSessionsRegistry
	error error
}

func NewUserManager() *UserManager {
	um := &UserManager{
		Users: make(map[string]*User),
		Mu:    &sync.RWMutex{},
	}
	// um.userCacheCleanup(4 * time.Hour)
	return um
}

func (um *UserManager) AddNewUser(sessionTokenExpiryTime time.Duration, refreshTokenExpiryTime time.Duration, memorylimitInMB float64) (*User, error) {

	if memorylimitInMB <= 0 {
		return nil, errMemoryLimit
	}

	var userSnapShot userSnapShot
	wg := &sync.WaitGroup{}
	var osmemorychannel = make(chan error, 1)

	wg.Add(1)

	go operatingSystemAvailableMemory(osmemorychannel, wg)

	mbtoUint := mbSizeToUINT(memorylimitInMB)
	tokens, err := newTokenStrings(2)
	if err != nil {
		return nil, errGuid
	}

	var sessionuser = &Session{}
	var userId string = tokens[1]

	userSnapShot.id = userId
	userSnapShot.currentSessionId = tokens[0]
	userSnapShot.isActive = true

	sessionuser.SessionId = tokens[0]

	gensessrefErr := (sessionuser).generateSessionRefreshToken(sessionTokenExpiryTime, refreshTokenExpiryTime)
	if gensessrefErr != nil {
		return nil, gensessrefErr
	}

	sessionuser.lastAccessed = time.Now()
	sessionuser.isActive = true
	sessionuser.cache = newCache[string, any]()

	wg.Wait()

	var remainingOsSpace = osavailableMemory.Load()
	userSnapShot.remainingSpace = remainingOsSpace

	if osmemerror := <-osmemorychannel; osmemerror != nil {
		return nil, osmemerror
	}
	if !compareConfigOsMem(remainingOsSpace, mbtoUint) {
		return nil, errMemExceeded
	}

	var sessionConfigChannel = make(chan sessionPoolConfigDTO, 1)
	var userdto = &userDTO{
		user:              userSnapShot,
		isNew:             true,
		sessionTokenToAdd: tokens[0],
		pool:              newSessionRegistry(),
	}

	wg.Add(1)
	go sessionPoolConfig(userdto, sessionConfigChannel, wg)

	usermemory := &memorylimit{
		configured:     mbtoUint,
		remainingSpace: remainingOsSpace,
	}

	newUser := &User{
		Id:               userId,
		Mu:               sync.RWMutex{},
		Sessions:         map[string]*Session{tokens[0]: sessionuser},
		CurrentSessionId: tokens[0],
		SharedCache:      newCache[string, any](),
		memory:           usermemory,
		pool:             newSessionRegistry(),
		isActive:         true,
	}

	wg.Wait()

	session := <-sessionConfigChannel
	if session.error != nil {
		return nil, session.error
	}
	newUser.Mu.Lock()
	session.pool.mu.RLock()
	newUser.pool.sessionIDs = session.pool.sessionIDs
	newUser.pool.currentSessions = 1
	session.pool.mu.RUnlock()
	newUser.Mu.Unlock()

	um.Mu.Lock()
	um.Users[userId] = newUser
	um.Mu.Unlock()
	return newUser, nil
}

func (um *UserManager) AddNewSessionToUser(userId string, sessionTokenExpiryTime time.Duration, refreshTokenExpiryTime time.Duration) (*User, error) {
	um.Mu.Lock()
	user, error := um.Users[userId]
	um.Mu.Unlock()
	if !error {
		return nil, errUser
	}

	userCopy := user.newUserSnapshot()

	err := userCopy.isUserAlive(userId)
	if err != nil {
		return nil, err
	}

	user.Mu.RLock()
	user.pool.mu.RLock()
	pool := &activeSessionsRegistry{
		currentSessions: user.pool.currentSessions,
		sessionIDs:      user.pool.sessionIDs,
	}
	user.pool.mu.RUnlock()
	user.Mu.RUnlock()

	wg := &sync.WaitGroup{}
	var sessionConfigChannel = make(chan sessionPoolConfigDTO, 1)
	var sizeCalculatorChannel = make(chan uint64, 1)

	wg.Add(2)

	go memoryCalculator(userCopy, sizeCalculatorChannel, wg)

	sessionIdUser, err := newTokenString()
	if err != nil {
		return nil, errGuid
	}

	var userdto = &userDTO{
		user:              userCopy,
		isNew:             false,
		sessionTokenToAdd: sessionIdUser,
		pool:              pool,
	}

	go sessionPoolConfig(userdto, sessionConfigChannel, wg)

	newsession := &Session{
		SessionId: sessionIdUser,
	}
	(newsession).generateSessionRefreshToken(sessionTokenExpiryTime, refreshTokenExpiryTime)
	newsession.lastAccessed = time.Now()
	newsession.isActive = true
	newsession.cache = newCache[string, any]()

	wg.Wait()
	session := <-sessionConfigChannel
	if session.error != nil {
		return nil, session.error
	}
	if userCopy.remainingSpace > <-sizeCalculatorChannel {
		user.Mu.Lock()
		user.Sessions[sessionIdUser] = newsession
		user.CurrentSessionId = sessionIdUser
		user.Mu.Unlock()
		return user, nil
	}
	return nil, errUserMem

}

func (u *User) AddSessionCache(key string, value any, ttl time.Duration) (*Session, error) {
	userCopy := u.newUserSnapshot()
	if userCopy.isActive {
		wg := &sync.WaitGroup{}
		var sizeCalculatorChannel = make(chan uint64, 1)

		wg.Add(1)

		go memoryCalculator(value, sizeCalculatorChannel, wg)

		sessionCopy := u.newSessionSnapshot()
		if sessionCopy.err != nil {
			return nil, sessionCopy.err
		}
		expired, expirederr := sessionCopy.checkTokenExpired()

		//need to look again whether err should be returned or need to add a strategy to resolve it
		switch expirederr {
		case errTokenGen:
			return nil, errTokenGen
		case errAuth:
			RetryAuthentication(&sessionCopy)
			expired, _ := sessionCopy.checkTokenExpired()
			if expired {
				return nil, errAuth
			}
			expired = false
			fallthrough
		default:
			if !expired {
				u.Mu.RLock()
				defer u.Mu.RUnlock()
				session := u.Sessions[userCopy.currentSessionId]
				session.mu.Lock()
				defer session.mu.Unlock()
				session.sessionToken = sessionCopy.sessionToken

				wg.Wait()
				if userCopy.remainingSpace > <-sizeCalculatorChannel {
					// u.Mu.Lock()
					// defer u.Mu.Unlock()
					// u.SharedCache.Store[userCopy.id] = cacheItem[any]{
					// 	Value:        value,
					// 	LastAccessed: time.Now(),
					// }
					session.cache.Mu.Lock()
					session.cache.Store[key] = cacheItem[any]{
						Value:        value,
						ExpiryTime:   time.Now().Add(ttl),
						LastAccessed: time.Now(),
					}
					session.cache.Mu.Unlock()

					return session, nil

				}
				return session, errCacheLimit
			}
		}
		wg.Wait()

	}
	return nil, errUserInactive

}

// func (u *User) AddorUpdateSessionCache(sessionid, sessionToken, key string, value any) (*session, error) {

// 	usercopy := u.newUserSnapshot()
// 	u.Mu.RLock()
// 	session, exists := u.Sessions[sessionid]
// 	u.Mu.RUnlock()
// 	if !exists {
// 		return nil, errSession
// 	}
// 	err := (session).checkTokenExpired()
// 	if err == errAuth {
// 		RetryAuthentication(session)
// 	}
// 	if sessionToken != session.sessionToken {
// 		return nil, errSessionToken
// 	}

// 	updatedsession := s.checkTokenExpired(sessionToken)
// 	switch {
// 	case updatedsession == nil:
// 		return updatedsession, errAddorUpdateCache
// 	case updatedsession.Err != nil:
// 		return updatedsession, updatedsession.Err
// 	}

// }

func AddorUpdateUserCache() {

}

func (user *User) FetchCacheData(key string) (any, error) {
	user.Mu.RLock()
	defer user.Mu.RUnlock()
	alive := user.isActive
	if alive {
		session, found := user.Sessions[user.CurrentSessionId]
		if found {
			if session.isActive {

				cache := session.cache
				cache.Mu.RLock()
				store, found := cache.Store[key]
				if found {
					cache.Mu.RUnlock()
					if !time.Now().After(store.ExpiryTime) {
						if store.Value != nil {
							return store.Value, nil
						}
						return nil, errCacheData
					} else {
						cache.deleteCache(key)
						return nil, errCacheExpired
					}
				}
				return nil, errReadCacheKey

			}
			return nil, errSessionInactive
		}
		return nil, errSession
	}
	return nil, errUserInactive
}

func (user *User) newUserSnapshot() userSnapShot {
	user.Mu.RLock()
	defer user.Mu.RUnlock()
	user.pool.mu.RLock()
	defer user.pool.mu.RUnlock()

	return userSnapShot{
		id:               user.Id,
		currentSessionId: user.CurrentSessionId,
		isActive:         user.isActive,
		remainingSpace:   user.memory.remainingSpace,
		sessions:         user.pool.sessionIDs[user.Id],
		currentSessions:  user.pool.currentSessions,
	}

}

func (user *User) newSessionSnapshot() sessionSnapshot {
	var sessionSnapCopy sessionSnapshot
	user.Mu.RLock()
	defer user.Mu.RUnlock()
	if user.isActive {
		session, found := user.Sessions[user.CurrentSessionId]
		if found {
			if session.isActive {
				sessionSnapCopy.sessionId = session.SessionId
				sessionSnapCopy.sessionToken = session.sessionToken
				sessionSnapCopy.refreshToken = session.refreshToken
				sessionSnapCopy.refreshExpiry = session.refreshExpiry
				sessionSnapCopy.sessionExpiry = session.sessionExpiry
				return sessionSnapCopy
			}
			sessionSnapCopy.err = errSessionInactive
			return sessionSnapCopy
		}
		sessionSnapCopy.err = errSession
		return sessionSnapCopy

	}
	sessionSnapCopy.err = errUserInactive
	return sessionSnapCopy
}

// func (c userPayload) hasAllNeededData(flag bool) bool {
// 	switch {
// 	case c.Id == "":
// 	case c.Key == "":
// 	case flag && c.Value == nil:
// 	default:
// 		return true
// 	}
// 	return false
// }

func (um *userSnapShot) isUserAlive(userid string) error {

	if !um.isActive {
		return errUserInactive
	}
	return nil
}

func (c *Cache[K, V]) deleteCache(key K) {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	delete(c.Store, key)
}
