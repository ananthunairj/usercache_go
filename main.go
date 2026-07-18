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
	anandhu, err := um.AddNewUser(30*time.Minute, 2*time.Minute, 30)
	if err != nil {

		//fmt.Print(err.Error())
		return
	}
	// fmt.Printf("%+v\n", anandhu)

	anandhu, err = um.AddNewSessionToUser(anandhu.Id, 40*time.Minute, 2*time.Minute)
	
	if err != nil {
		//fmt.Print(err.Error())
		return
	}
	//fmt.Printf("%+v/n", ak)

	_,err = anandhu.AddSessionCache("fruits","apple,guava",2*time.Minute)
	if err != nil {
		return
	}
	value,_ := anandhu.FetchCacheData("fruits")
		fmt.Print(value)

	

}
