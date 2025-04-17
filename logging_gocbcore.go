package cbcolumnar

import "github.com/couchbase/gocbcore/v10"

type coreLogWrapper struct {
	wrapped gocbcore.Logger
}

func (wrapper coreLogWrapper) Log(level LogLevel, offset int, format string, v ...interface{}) error {
	return wrapper.wrapped.Log(gocbcore.LogLevel(level), offset+2, format, v...) // nolint:wrapcheck
}

type coreLogger struct {
	wrapped Logger
}

func (wrapper coreLogger) Log(level gocbcore.LogLevel, offset int, format string, v ...interface{}) error {
	return wrapper.wrapped.Log(LogLevel(level), offset+2, format, v...) // nolint:wrapcheck
}

func getCoreLogger(logger Logger) gocbcore.Logger {
	typedLogger, isCoreLogger := logger.(*coreLogWrapper)
	if isCoreLogger {
		return typedLogger.wrapped
	}

	return &coreLogger{
		wrapped: logger,
	}
}
