package src

import (
	"github.com/streamonkey/size"
	"sync"
	"sync/atomic"
)

type memorylimit struct {
	configured     uint64
	remainingSpace uint64
}

type activeSessionsRegistry struct {
	mu              sync.RWMutex
	currentSessions uint8
	sessionIDs      map[string][]string //key here is the user name
}

type activeUserSession struct {
	// mu         sync.RWMutex
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
	switch v := value.(type) {
	case []byte:
		c <- uint64(len(v))
		return
	case string:
		c <- uint64(len(v))
		return
	case []string:
		totalSize := uint64(0)
		for _, s := range v {
			totalSize += uint64(len(s))
		}
		c <- totalSize
		return
	default:
		c <- uint64(size.Of(value))
		return

	}
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
		userinpool, userinpoolexist := userdto.pool.sessionIDs[userdto.user.id]
		userdto.pool.mu.RUnlock()

		if !userdto.isNew {
			if userinpoolexist {
				if len(userinpool) < Allowed_Sessions {
					userdto.pool.mu.Lock()
					userinpool = append(userinpool, userdto.sessionTokenToAdd)
					userdto.pool.mu.Unlock()
					var activeSessionRegistryObj = &activeSessionsRegistry{}
					activeSessionRegistryObj.sessionIncrementer()
					activeSessionRegistryObj.sessionIDs[userdto.user.id] = userinpool

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
			var activeSessionRegistryObj = &activeSessionsRegistry{
				sessionIDs: make(map[string][]string),
			}

			activeSessionRegistryObj.sessionIDs[userdto.user.id] = append(activeSessionRegistryObj.sessionIDs[userdto.user.id], userdto.sessionTokenToAdd)
			activeSessionRegistryObj.sessionIncrementer()

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
		sessionIDs: make(map[string][]string),
	}
}

func (a *activeSessionsRegistry) sessionIncrementer() {
	//lock here is experimental, will remove if no future routine acts up on this struct
	a.mu.Lock()
	a.currentSessions++
	a.mu.Unlock()
}
