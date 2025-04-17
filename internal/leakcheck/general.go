// Package leakcheck provides utilities for detecting and reporting goroutine leaks
// in Go applications.
// It includes functions to enable leak checking and report any detected leaks.
//
// The package is designed to be used in test environments to ensure that no
// goroutines are left running after tests have completed, which can help
// identify and fix resource leaks in the code.
package leakcheck

// EnableAll enables all leak checking.
func EnableAll() {
	PrecheckGoroutines()
}

// ReportAll reports all leak checking.
func ReportAll() bool {
	testsPassed := true

	if !ReportLeakedGoroutines() {
		testsPassed = false
	}

	return testsPassed
}
