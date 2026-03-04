package src

import "time"

//dummy refresh token, you can remove this and add refresh token from an OKTA / application / custom one whatever it is!
var dumpRefreshToken = "CAjVTFhs9DszFJ-Iym7gGVHd92MDZSiQCPyHcfUc8qI="
//dummy refresh token Expire time, should change based on your requirements
var dummyexpiry time.Duration = 6 * time.Hour 


func RetryAuthentication(session *sessionSnapshot) {
  //change these with your credentials
  session.refreshExpiry = time.Now().Add(dummyexpiry)
  session.refreshToken = dumpRefreshToken

}