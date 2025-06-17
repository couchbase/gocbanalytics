package cbanalytics_test

import (
	"testing"

	cbanalytics "github.com/couchbase/gocbanalytics"
	"github.com/stretchr/testify/assert"
)

func TestInvalidScheme(t *testing.T) {
	_, err := cbanalytics.NewCluster("couchbase://localhost", cbanalytics.NewBasicAuthCredential("username", "password"), DefaultOptions())

	assert.ErrorIs(t, err, cbanalytics.ErrInvalidArgument)
}

func TestNoScheme(t *testing.T) {
	_, err := cbanalytics.NewCluster("//localhost", cbanalytics.NewBasicAuthCredential("username", "password"), DefaultOptions())

	assert.ErrorIs(t, err, cbanalytics.ErrInvalidArgument)
}
