package cbanalytics

// UserPassPair represents a username and password pair.
type UserPassPair struct {
	Username string
	Password string
}

// DynamicBasicAuthCredential provides a way to authenticate using a dynamic username and password.
type DynamicBasicAuthCredential struct {
	Provider func() UserPassPair
}

func (u *DynamicBasicAuthCredential) isCredential() {}

func (u *DynamicBasicAuthCredential) Credentials() UserPassPair {
	return u.Provider()
}

// NewDynamicBasicAuthCredential creates a new DynamicBasicAuthCredential with the specified provider function.
func NewDynamicBasicAuthCredential(provider func() UserPassPair) *DynamicBasicAuthCredential {
	return &DynamicBasicAuthCredential{
		Provider: provider,
	}
}

// BasicAuthCredential provides a way to authenticate using username and password.
type BasicAuthCredential struct {
	UserPassPair UserPassPair
}

func (u *BasicAuthCredential) isCredential() {}

// Credentials returns the UserPassPair for BasicAuthCredential.
func (u *BasicAuthCredential) Credentials() UserPassPair {
	return u.UserPassPair
}

// NewBasicAuthCredential creates a new BasicAuthCredential with the specified username and password.
func NewBasicAuthCredential(username, password string) *BasicAuthCredential {
	return &BasicAuthCredential{
		UserPassPair: UserPassPair{Username: username, Password: password},
	}
}

// Credential provides a way to authenticate with the server.
type Credential interface {
	isCredential()
}
