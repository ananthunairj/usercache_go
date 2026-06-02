package main

import (
	"fmt"
	"time"

	"github.com/ananthunairj/usercache_go/src"
)

type Person struct {
	Name      string   `json:"full_name"`
	Age       int      `json:"age"`
	IsStudent bool     `json:"is_student"`
	Courses   []string `json:"courses"`
}

func main() {

	um := src.NewUserManager()
	anandhu, err := um.AddNewUser(30*time.Second, 2*time.Minute, 30)
	if err != nil {

		fmt.Print(err.Error())
	}
	fmt.Printf("%+v\n", anandhu)

	ak, err := um.AddNewSessionToUser(anandhu.Id, 40*time.Second, 2*time.Minute)
	if err != nil {
		fmt.Print(err.Error())
	}
	fmt.Printf("%+v/n", ak)

}
