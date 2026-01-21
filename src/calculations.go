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
	mu              sync.RWMutex
	currentSessions uint8
	sessions        map[string]*activeUserSession //key here is the user name
}

type activeUserSession struct {
	mu         sync.RWMutex
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

func sessionPoolConfig(userdto *userDTO, c chan<- sessionPoolConfigDTO, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	defer close(c)

	if userdto.user.isActive == true {

		//experimental may be moved to inside the loop  based on behaviour if needed
		userdto.pool.mu.RLock()
		userinpool, userinpoolexist := userdto.pool.sessions[userdto.user.id]
		userdto.pool.mu.RUnlock()

		if !userdto.isNew {
			if userinpoolexist {
				if len(userinpool.SessionIDs) < Allowed_Sessions {
					userinpool.mu.Lock()
					userinpool.SessionIDs = append(userinpool.SessionIDs, userdto.sessionTokenToAdd)
					userinpool.mu.Unlock()
					var activeSessionRegistryObj = &activeSessionsRegistry{}
					activeSessionRegistryObj.sessionIncrementer()
					activeSessionRegistryObj.sessions[userdto.user.id] = userinpool

					c <- sessionPoolConfigDTO{
						pool: activeSessionRegistryObj,
					}
					return
				}
				c <- sessionPoolConfigDTO{
					pool:  nil,
					error: errSessionLimit,
				}
				return
			}
			c <- sessionPoolConfigDTO{
				pool:  nil,
				error: errUserInRegistryNotFound,
			}
			return
		} else if userdto.isNew && !userinpoolexist {
			var activeSessionRegistryObj = &activeSessionsRegistry{}
			var activeSessionObj = &activeUserSession{}
			activeSessionObj.SessionIDs = append(activeSessionObj.SessionIDs, userdto.sessionTokenToAdd)
			activeSessionRegistryObj.sessionIncrementer()
			activeSessionRegistryObj.sessions[userdto.user.id] = activeSessionObj

			c <- sessionPoolConfigDTO{
				pool: activeSessionRegistryObj,
			}
			return
		}
		c <- sessionPoolConfigDTO{
			pool:  nil,
			error: errUserDto,
		}
		return
	}
	c <- sessionPoolConfigDTO{
		pool:  nil,
		error: errUser,
	}
	return

}

func newSessionRegistry() *activeSessionsRegistry {
	return &activeSessionsRegistry{
		sessions: make(map[string]*activeUserSession),
	}
}

func (a *activeSessionsRegistry) sessionIncrementer() {
	//lock here is experimental, will remove if no future routine acts up on this struct
	a.mu.Lock()
	a.currentSessions++
	a.mu.Unlock()
}
