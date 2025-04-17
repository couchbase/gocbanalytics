package cbcolumnar_test

import (
	"testing"

	cbcolumnar "github.com/couchbase/gocbcolumnar"
	"github.com/stretchr/testify/assert"
)

func TestInvalidCipherSuites(t *testing.T) {
	opts := DefaultOptions().SetSecurityOptions(cbcolumnar.NewSecurityOptions().SetCipherSuites([]string{"bad"}))
	_, err := cbcolumnar.NewCluster("couchbases://localhost", cbcolumnar.NewCredential("username", "password"), opts)

	assert.ErrorIs(t, err, cbcolumnar.ErrInvalidArgument)
}

func TestInvalidScheme(t *testing.T) {
	_, err := cbcolumnar.NewCluster("couchbase://localhost", cbcolumnar.NewCredential("username", "password"), DefaultOptions())

	assert.ErrorIs(t, err, cbcolumnar.ErrInvalidArgument)
}

func TestNoScheme(t *testing.T) {
	_, err := cbcolumnar.NewCluster("//localhost", cbcolumnar.NewCredential("username", "password"), DefaultOptions())

	assert.ErrorIs(t, err, cbcolumnar.ErrInvalidArgument)
}
