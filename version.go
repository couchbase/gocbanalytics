package cbanalytics

const (
	versionStr = "v1.1.0"
)

// Version returns a string representation of the current SDK version.
func Version() string {
	return versionStr
}

// Identifier returns a string representation of the current SDK identifier.
func Identifier() string {
	return "gocb-analytics/" + versionStr
}
