package src

import (
	"sync"
	"sync/atomic"

	"github.com/streamonkey/size"
)

type memorylimit struct {
	configured     uint64
	remainingSpace uint64
}

type activeSessionsRegistry struct {
	mu          sync.RWMutex
	maxSessions uint8
	users       map[string]*activeUserSession
}

type activeUserSession struct {
	SessionIDs []string
}

type registrySessionDTO struct {
	userid            string
	sessionTokenToAdd string
}

var osavailableMemory atomic.Uint64

func calculateInputBytes(value any, c chan<- uint64, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	defer close(c)
	bytevalue := size.Of(value)
	c <- uint64(bytevalue)
}

func mbSizeToUINT(value float64) uint64 {
	 return uint64(value * mbtouintsize)
	
}

func compareConfigOsMem(osmem uint64, configmem uint64) bool {
	osmem -= osmem * Memory_cutoff / 100
	if osmem > configmem {
		return true
	}
	return false
}

func sessionPoolConfig(userdto *userDTO, c chan<- error, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	defer close(c)

	if userdto.user.isActive == true {
		var pool *activeSessionsRegistry
		pool.mu.RLock()
		userinpool, userinpoolexist := pool.users[userdto.user.id]
		pool.mu.RUnlock()

		if !userdto.isNew {
			if userinpoolexist {
				if len(userinpool.SessionIDs) < Allowed_Sessions {
					pool.mu.Lock()
					userinpool.SessionIDs = append(userinpool.SessionIDs, userdto.sessionTokenToAdd)
					pool.mu.Unlock()
					c <- nil
					return
				}
				c <- errSessionLimit
				return
			}
			registrydto := registrySessionDTO{
				userid:            userdto.user.id,
				sessionTokenToAdd: userdto.sessionTokenToAdd,
			}
			newRegistryAssigner(registrydto, pool)
			c <- nil
			return
		} else if userdto.isNew && !userinpoolexist {
			registrydto := registrySessionDTO{
				userid:            userdto.user.id,
				sessionTokenToAdd: userdto.sessionTokenToAdd,
			}
			newRegistryAssigner(registrydto, pool)
			c <- nil
			return
		}
		c <- errUserDto
		return
	}
	c <- errUser
	return

}

func newRegistryAssigner(userdto registrySessionDTO, pool *activeSessionsRegistry) {
	newActiveUserSession := &activeUserSession{
		SessionIDs: []string{userdto.sessionTokenToAdd},
	}
	pool.mu.Lock()
	pool.users[userdto.userid] = newActiveUserSession
	pool.mu.Unlock()
}
