package src

import (
	"time"
)

const (
	DefaultSessionTokenExpiry = DefaultSessionTokenTime * time.Minute
	DefaultRefreshTokenExpiry = DefaultRefreshTokenTime * time.Hour
)

func (session *session) generateSessionRefreshToken(sessionTokenExpiryTime time.Duration, refreshTokenExpiryTime time.Duration) error {
	sessionToken, sessiontokenerr := generateToken(session_token_length)
	if sessiontokenerr != nil {
		return errSessionTokenGen
	}
	session.mu.Lock()
	session.sessionToken = sessionToken
	session.mu.Unlock()

	refreshToken, refreshtokenerr := generateToken(refresh_token_length)
	if refreshtokenerr != nil {
		return errRefershTokenGen
	}
	session.mu.Lock()
	session.refreshToken = refreshToken
	session.mu.Unlock()

	if sessionTokenExpiryTime <= 0 {
		session.mu.Lock()
		session.sessionExpiry = time.Now().Add(DefaultSessionTokenExpiry)
		session.mu.Unlock()
	} else {
		session.mu.Lock()
		session.sessionExpiry = time.Now().Add(sessionTokenExpiryTime)
		session.mu.Unlock()
	}
	if refreshTokenExpiryTime <= 0 {
		session.mu.Lock()
		session.refreshExpiry = time.Now().Add(DefaultRefreshTokenExpiry)
		session.mu.Unlock()
	} else {
		session.mu.Lock()
		session.refreshExpiry = time.Now().Add(refreshTokenExpiryTime)
		session.mu.Unlock()
	}
	return nil
}

func (s *sessionSnapshot) checkTokenExpired() (bool, error) {

	if time.Now().After(s.sessionExpiry) {
		if !time.Now().After(s.refreshExpiry) {
			var sessionToken string
			var err error
			for try := 0; try <= max_tokengen_try; try++ {
				sessionToken, err = generateToken(session_token_length)
				if err == nil {
					break
				}
			}
			if err != nil {
				s.sessionToken = ""
				return true, errTokenGen

			}
			s.sessionToken = sessionToken
			return true, nil
		}

		s.sessionToken = ""
		return true, errAuth
	}
	return false, nil
}

func (u *User) verifySessionCredentials(sessionid string, sessiontoken string) error {

	
	sessionCopy := u.newSessionSnapshot(sessionid)
	if sessionCopy.err != nil {
		return sessionCopy.err
	}
	
	if sessionCopy.sessionToken != "" {
		// if u.CurrentSessionId == sessionid {

		// }
		expired,err := sessionCopy.checkTokenExpired()
		if expired {
              return err
		}
		return err
	}
	return errSession

}
