package main

import (
	"fmt"

	"github.com/ananthunairj/usercache_go/src"
)

type Person struct {
	Name      string   `json:"full_name"`
	Age       int      `json:"age"`
	IsStudent bool     `json:"is_student"`
	Courses   []string `json:"courses"`
}

func main() {

	 token,_ := src.GenerateToken(src.REFRESH_TOKEN_LENGTH)
	 fmt.Print(token)
	
}
