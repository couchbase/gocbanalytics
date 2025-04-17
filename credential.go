package cbcolumnar

// UserPassPair represents a username and password pair.
type UserPassPair struct {
	Username string
	Password string
}

// Credential provides a way to specify credentials to the SDK.
type Credential struct {
	UsernamePassword *UserPassPair
}

// NewCredential creates a new Credential with the specified username and password.
func NewCredential(username, password string) Credential {
	return Credential{
		UsernamePassword: &UserPassPair{Username: username, Password: password},
	}
}
