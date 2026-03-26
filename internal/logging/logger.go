// Package logging provides a simple logging interface and a default implementation using the standard log package.
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

// NewDefaultLogger creates a new DefaultLogger with the given log level and offset.
func NewDefaultLogger(level LogLevel, offset int) *DefaultLogger {
	return &DefaultLogger{
		Level:    level,
		GoLogger: log.New(os.Stderr, "cbanalytics ", log.Lmicroseconds|log.Lshortfile),
		Offset:   offset,
	}
}

// Error outputs an error level log message.
func (l *DefaultLogger) Error(format string, v ...interface{}) {
	l.Log(LogError, format, v...)
}

// Warn outputs an warn level log message.
func (l *DefaultLogger) Warn(format string, v ...interface{}) {
	l.Log(LogWarn, format, v...)
}

// Info outputs an info level log message.
func (l *DefaultLogger) Info(format string, v ...interface{}) {
	l.Log(LogInfo, format, v...)
}

// Debug outputs a debug level error log message.
func (l *DefaultLogger) Debug(format string, v ...interface{}) {
	l.Log(LogDebug, format, v...)
}

// Trace outputs a trace level error log message.
func (l *DefaultLogger) Trace(format string, v ...interface{}) {
	l.Log(LogTrace, format, v...)
}

// Log outputs a log message with the given log level and format.
func (l *DefaultLogger) Log(level LogLevel, format string, v ...interface{}) {
	if level > l.Level {
		return
	}

	s := fmt.Sprintf(format, v...)

	err := l.GoLogger.Output(5, s)
	if err != nil {
		log.Printf("Logger error occurred (%s)\n", err)
	}
}
