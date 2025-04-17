package cbanalytics_test

import (
	"testing"

	cbanalytics "github.com/couchbase/gocbanalytics"
	"github.com/stretchr/testify/assert"
)

func TestInvalidCipherSuites(t *testing.T) {
	opts := DefaultOptions().SetSecurityOptions(cbanalytics.NewSecurityOptions().SetCipherSuites([]string{"bad"}))
	_, err := cbanalytics.NewCluster("couchbases://localhost", cbanalytics.NewCredential("username", "password"), opts)

	assert.ErrorIs(t, err, cbanalytics.ErrInvalidArgument)
}

func TestInvalidScheme(t *testing.T) {
	_, err := cbanalytics.NewCluster("couchbase://localhost", cbanalytics.NewCredential("username", "password"), DefaultOptions())

	assert.ErrorIs(t, err, cbanalytics.ErrInvalidArgument)
}

func TestNoScheme(t *testing.T) {
	_, err := cbanalytics.NewCluster("//localhost", cbanalytics.NewCredential("username", "password"), DefaultOptions())

	assert.ErrorIs(t, err, cbanalytics.ErrInvalidArgument)
}
