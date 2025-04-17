package logging

import (
	"fmt"
	"log"
	"os"
)

// LogLevel specifies the severity of a log message.
type LogLevel int

// Various logging levels (or subsystems) which can categorize the message.
// Currently these are ordered in decreasing severity.
const (
	LogError LogLevel = iota
	LogWarn
	LogInfo
	LogDebug
	LogTrace
)

// Logger defines a logging interface.
type Logger interface {
	// Error outputs an error level log message.
	Error(format string, v ...interface{})

	// Warn outputs an warn level log message.
	Warn(format string, v ...interface{})

	// Info outputs an info level log message.
	Info(format string, v ...interface{})

	// Debug outputs a debug level error log message.
	Debug(format string, v ...interface{})

	// Trace outputs a trace level error log message.
	Trace(format string, v ...interface{})
}

// DefaultLogger is a simple logger that uses the standard log package.
type DefaultLogger struct {
	Level    LogLevel
	GoLogger *log.Logger
	Offset   int
}

func NewDefaultLogger(level LogLevel, offset int) *DefaultLogger {
	return &DefaultLogger{
		Level:    level,
		GoLogger: log.New(os.Stderr, "gocbanalytics ", log.Lmicroseconds|log.Lshortfile),
		Offset:   offset,
	}
}

func (l *DefaultLogger) Error(format string, v ...interface{}) {
	l.Log(LogError, format, v...)
}

func (l *DefaultLogger) Warn(format string, v ...interface{}) {
	l.Log(LogWarn, format, v...)
}

func (l *DefaultLogger) Info(format string, v ...interface{}) {
	l.Log(LogInfo, format, v...)
}

func (l *DefaultLogger) Debug(format string, v ...interface{}) {
	l.Log(LogDebug, format, v...)
}

func (l *DefaultLogger) Trace(format string, v ...interface{}) {
	l.Log(LogTrace, format, v...)
}

func (l *DefaultLogger) Log(level LogLevel, format string, v ...interface{}) {
	if level > l.Level {
		return
	}

	s := fmt.Sprintf(format, v...)

	err := l.GoLogger.Output(l.Offset+2, s)
	if err != nil {
		log.Printf("Logger error occurred (%s)\n", err)
	}
}
