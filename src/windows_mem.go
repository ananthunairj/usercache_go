//go:build windows
// +build windows

package src

import (
	"sync"
	"syscall"
	"unsafe"
)

type memoryStatusEx struct {
	dwLength                uint32
	dwMemoryLoad            uint32
	ullTotalPhys            uint64
	ullAvailPhys            uint64
	ullTotalPageFile        uint64
	ullAvailPageFile        uint64
	ullTotalVirtual         uint64
	ullAvailVirtual         uint64
	ullAvailExtendedVirtual uint64
}

var (
	kernel32             = syscall.NewLazyDLL("kernel32.dll")
	globalMemorystatusEx = kernel32.NewProc("GlobalMemoryStatusEx")
	memoryStatus         memoryStatusEx
)

func operatingSystemAvailableMemory(c chan<- error, wg *sync.WaitGroup)  {
	defer wg.Done()
	memoryStatus.dwLength = uint32(unsafe.Sizeof(memoryStatus))
	ret, _, callErr := globalMemorystatusEx.Call(uintptr(unsafe.Pointer(&memoryStatus)))
	if ret == 0 {
		_ = osavailableMemory.Swap(0)
		if callErr != nil && callErr != syscall.Errno(0) {
            c <- callErr
            return
        }
		c <- errGlobalMemoryStatusEx
		return 
	}
	_ = osavailableMemory.Swap(memoryStatus.ullAvailPhys)
	c <- nil
}
