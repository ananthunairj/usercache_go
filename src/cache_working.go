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
	Sessions         map[string]*session
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

type session struct {
	sessionId     string
	sessionToken  string
	refreshToken  string
	isActive      bool
	sessionExpiry time.Time
	refreshExpiry time.Time
	lastAccessed  time.Time
	cache         *Cache[string, any]
	mu            sync.RWMutex
}

type cacheItem[T any] struct {
	Value        T
	ExpiryTime   time.Time
	LastAccessed time.Time
}

type Cache[K comparable, V any] struct {
	Mu    sync.Mutex
	Store map[K]cacheItem[V]
}

func newCache[K comparable, V any]() *Cache[K, V] {
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
	pool              activeSessionsRegistry
}

type sessionPoolConfigDTO struct {
	pool  *activeSessionsRegistry
	error error
}

func NewUserManager() *UserManager {
	um := &UserManager{
		Users: make(map[string]*User),
	}
	// um.userCacheCleanup(4 * time.Hour)
	return um
}

func (um *UserManager) AddNewUser(sessionTokenExpiryTime time.Duration, refreshTokenExpiryTime time.Duration, memorylimitInMB float64) (*User, error) {

	if memorylimitInMB <= 0 {
		return nil, errMemoryLimit
	}

	var userSnapShot userSnapShot
	var wg *sync.WaitGroup
	var osmemorychannel = make(chan error, 1)

	wg.Add(1)

	go operatingSystemAvailableMemory(osmemorychannel, wg)

	mbtoUint := mbSizeToUINT(memorylimitInMB)
	tokens, err := newTokenStrings(2)
	if err != nil {
		return nil, errGuid
	}

	var sessionuser = &session{}
	var userId string = tokens[1]

	userSnapShot.id = userId
	userSnapShot.currentSessionId = tokens[0]
	userSnapShot.isActive = true

	sessionuser.sessionId = tokens[0]

	gensessrefErr := (sessionuser).generateSessionRefreshToken(sessionTokenExpiryTime, refreshTokenExpiryTime)
	if gensessrefErr != nil {
		return nil, gensessrefErr
	}
	if err != nil {
		return nil, errTokenGen
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
		pool:              *newSessionRegistry(),
	}

	wg.Add(1)
	go sessionPoolConfig(userdto, sessionConfigChannel, wg)

	usermemory := &memorylimit{
		configured:     mbtoUint,
		remainingSpace: remainingOsSpace,
	}

	newUser := &User{
		Id:               userId,
		Sessions:         map[string]*session{tokens[0]: sessionuser},
		CurrentSessionId: tokens[0],
		SharedCache:      newCache[string, any](),
		memory:           usermemory,
		isActive:         true,
	}

	wg.Wait()

	session := <-sessionConfigChannel
	if session.error != nil {
		return nil, session.error
	}

	session.pool.mu.RLock()
	newUser.pool.sessionIDs = session.pool.sessionIDs
	session.pool.mu.RUnlock()

	um.Mu.Lock()
	um.Users[userId] = newUser
	um.Mu.Unlock()
	return newUser, nil
}

func (um *UserManager) AddNewSessionToUser(userId string, sessionTokenExpiryTime time.Duration, refreshTokenExpiryTime time.Duration) (*session, error) {
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

	var wg *sync.WaitGroup
	var sessionConfigChannel = make(chan sessionPoolConfigDTO, 1)
	var sizeCalculatorChannel = make(chan uint64, 1)

	wg.Add(2)

	go calculateInputBytes(userCopy, sizeCalculatorChannel, wg)

	sessionId, err := newTokenString()
	if err != nil {
		return nil, errGuid
	}

	var userdto = &userDTO{
		user:              userCopy,
		isNew:             false,
		sessionTokenToAdd: sessionId,
	}

	go sessionPoolConfig(userdto, sessionConfigChannel, wg)

	var newsession *session
	newsession.sessionId = sessionId
	(newsession).generateSessionRefreshToken(sessionTokenExpiryTime, refreshTokenExpiryTime)
	newsession.lastAccessed = time.Now()
	newsession.cache = newCache[string, any]()

	wg.Wait()
	session := <-sessionConfigChannel
	if session.error != nil {
		return nil, session.error
	}
	if userCopy.remainingSpace > <-sizeCalculatorChannel {
		user.Mu.Lock()
		user.Sessions[sessionId] = newsession
		user.CurrentSessionId = sessionId
		user.Mu.Unlock()
		return newsession, nil
	}
	return nil, errUserMem

}

func (u *User) AddSessionCache(sessionid, key string, value any) (*session, error) {

	usercopy := u.newUserSnapshot()

}

func (u *User) UpdateSessionCache() (*session, error) {

}

func (u *User) AddorUpdateSessionCache(sessionid, sessionToken, key string, value any) (*session, error) {

	usercopy := u.newUserSnapshot()
	u.Mu.RLock()
	session, exists := u.Sessions[sessionid]
	u.Mu.RUnlock()
	if !exists {
		return nil, errSession
	}
	err := (session).checkTokenExpired()
	if err == errAuth {
		RetryAuthentication(session)
	}
	if sessionToken != session.sessionToken {
		return nil, errSessionToken
	}

	updatedsession := s.checkTokenExpired(sessionToken)
	switch {
	case updatedsession == nil:
		return updatedsession, errAddorUpdateCache
	case updatedsession.Err != nil:
		return updatedsession, updatedsession.Err
	}

}

func AddorUpdateUserCache() {

}

func (user *User) newUserSnapshot() userSnapShot {
	user.Mu.RLock()
	defer user.pool.mu.RUnlock()
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
