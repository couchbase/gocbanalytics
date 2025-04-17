package ganalytics_test

import (
	"testing"

	ganalytics "github.com/couchbase/ganalytics"
	"github.com/stretchr/testify/assert"
)

func TestInvalidCipherSuites(t *testing.T) {
	opts := DefaultOptions().SetSecurityOptions(ganalytics.NewSecurityOptions().SetCipherSuites([]string{"bad"}))
	_, err := ganalytics.NewCluster("couchbases://localhost", ganalytics.NewCredential("username", "password"), opts)

	assert.ErrorIs(t, err, ganalytics.ErrInvalidArgument)
}

func TestInvalidScheme(t *testing.T) {
	_, err := ganalytics.NewCluster("couchbase://localhost", ganalytics.NewCredential("username", "password"), DefaultOptions())

	assert.ErrorIs(t, err, ganalytics.ErrInvalidArgument)
}

func TestNoScheme(t *testing.T) {
	_, err := ganalytics.NewCluster("//localhost", ganalytics.NewCredential("username", "password"), DefaultOptions())

	assert.ErrorIs(t, err, ganalytics.ErrInvalidArgument)
}
