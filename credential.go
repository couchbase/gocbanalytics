package cbanalytics

import (
	"crypto/tls"
	"sync/atomic"
)

// UserPassPair represents a username and password pair.
type UserPassPair struct {
	Username string
	Password string
}

// DynamicBasicAuthCredential provides a way to authenticate using a dynamic username and password.
//
// Deprecated: This function is deprecated and will be removed in a future release.
// Use Cluster.SetCredential and BasicAuthCredential instead.
type DynamicBasicAuthCredential struct {
	Provider func() UserPassPair
}

func (u *DynamicBasicAuthCredential) isCredential() {}

// Credentials returns the UserPassPair provided by the Provider function for DynamicBasicAuthCredential.
func (u *DynamicBasicAuthCredential) Credentials() UserPassPair {
	return u.Provider()
}

// NewDynamicBasicAuthCredential creates a new DynamicBasicAuthCredential with the specified provider function.
//
// Deprecated: This function is deprecated and will be removed in a future release.
// Use Cluster.SetCredential and BasicAuthCredential instead.
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

// CertificateCredential provides a way to authenticate using a client TLS certificate.
type CertificateCredential struct {
	ClientCertificate *tls.Certificate
}

func (c *CertificateCredential) isCredential() {}

// NewCertificateCredential creates a new CertificateCredential with the specified client certificate.
func NewCertificateCredential(clientCertificate *tls.Certificate) *CertificateCredential {
	return &CertificateCredential{
		ClientCertificate: clientCertificate,
	}
}

// JWTCredential provides a way to authenticate using a JWT token.
type JWTCredential struct {
	Token string
}

func (j *JWTCredential) isCredential() {}

// NewJWTCredential creates a new JWTCredential with the specified token.
func NewJWTCredential(token string) *JWTCredential {
	return &JWTCredential{
		Token: token,
	}
}

// Credential provides a way to authenticate with the server.
type Credential interface {
	isCredential()
}

type credentialStore struct {
	credential atomic.Value
}

func newCredentialStore(cred Credential) *credentialStore {
	s := &credentialStore{
		credential: atomic.Value{},
	}
	s.credential.Store(cred)

	return s
}

func (s *credentialStore) get() Credential {
	return s.credential.Load().(Credential)
}

func (s *credentialStore) set(cred Credential) {
	s.credential.Store(cred)
}
