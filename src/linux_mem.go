//go:build linux
// +build linux

package src

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"sync"
)

func operatingSystemAvailableMemory(c chan<- error, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	defer close(c)
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		_ = osavailableMemory.Swap(0)
		c <- errProcMem
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, freeMemoryConst) {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				value, err := strconv.ParseInt(fields[1], parse_start, parse_stop)
				if err != nil {
					_ = osavailableMemory.Swap(0)
					c <- errParseProcMem
					return
				}
				result := uint64(value * binary_sys_number)
				_ = osavailableMemory.Swap(result)
				c <- nil
				return
			}
		}
	}
	if err := scanner.Err(); err != nil {
		_ = osavailableMemory.Swap(0)
		c <- errReadProcMem
		return
	}
	_ = osavailableMemory.Swap(0)
	c <- errKeyNotFoundInProcMem

}
