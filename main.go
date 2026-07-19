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
	anandhu, err := um.AddNewUser(15*time.Second, 2*time.Second, 30)
	if err != nil {

		//fmt.Print(err.Error())
		return
	}
	// fmt.Printf("%+v\n", anandhu)

	
	//fmt.Printf("%+v/n", ak)

	_,err = anandhu.AddSessionCache("fruits","apple,guava",10*time.Second)
	if err != nil {
		return
	}
	value,_ := anandhu.FetchCacheData("fruits")
		fmt.Print(value)

	myTimer := time.NewTimer(14 * time.Second)

	fmt.Println("Timer started...")

	// This blocks until the timer's channel 'C' receives a value
	<-myTimer.C
	
	
	// value,_ = anandhu.FetchCacheData("fruits")
	// 	fmt.Print(value)

	_,err = anandhu.AddSessionCache("fruit","applet,banana",10*time.Second)
	if err != nil {
		fmt.Print(err.Error())
	}
    
	value,_ = anandhu.FetchCacheData("fruit")
		fmt.Print(value)
	

}
