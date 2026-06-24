package cbanalytics_test

import (
	"testing"

	cbanalytics "github.com/couchbase/gocbanalytics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvalidScheme(t *testing.T) {
	_, err := cbanalytics.NewCluster("couchbase://localhost", cbanalytics.NewBasicAuthCredential("username", "password"), DefaultOptions())

	assert.ErrorIs(t, err, cbanalytics.ErrInvalidArgument)
}

func TestNoScheme(t *testing.T) {
	_, err := cbanalytics.NewCluster("//localhost", cbanalytics.NewBasicAuthCredential("username", "password"), DefaultOptions())

	assert.ErrorIs(t, err, cbanalytics.ErrInvalidArgument)
}

// TestSetCredential_SameType verifies that SetCredential accepts a credential of the same
// type as the one originally provided to NewCluster.
func TestSetCredential_SameType(t *testing.T) {
	cluster, err := cbanalytics.NewCluster("http://localhost", cbanalytics.NewBasicAuthCredential("user", "pass"), DefaultOptions())
	require.NoError(t, err)

	defer cluster.Close()

	err = cluster.SetCredential(cbanalytics.NewBasicAuthCredential("newuser", "newpass"))
	assert.NoError(t, err)
}

// TestSetCredential_DifferentType verifies that SetCredential rejects a credential of a
// different type than the one originally provided to NewCluster.
func TestSetCredential_DifferentType(t *testing.T) {
	cluster, err := cbanalytics.NewCluster("https://localhost", cbanalytics.NewBasicAuthCredential("user", "pass"), DefaultOptions())
	require.NoError(t, err)

	defer cluster.Close()

	err = cluster.SetCredential(cbanalytics.NewJWTCredential("token"))
	assert.ErrorIs(t, err, cbanalytics.ErrInvalidArgument)
}

// TestSetCredential_Nil verifies that SetCredential rejects a nil credential.
func TestSetCredential_Nil(t *testing.T) {
	cluster, err := cbanalytics.NewCluster("http://localhost", cbanalytics.NewBasicAuthCredential("user", "pass"), DefaultOptions())
	require.NoError(t, err)

	defer cluster.Close()

	err = cluster.SetCredential(nil)
	assert.ErrorIs(t, err, cbanalytics.ErrInvalidArgument)
}
