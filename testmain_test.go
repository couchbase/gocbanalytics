package cbanalytics_test

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cbanalytics "github.com/couchbase/gocbanalytics"
	"github.com/couchbase/gocbanalytics/internal/leakcheck"
)

var TestOpts TestOptions

type TestOptions struct {
	Username        string
	Password        string
	OriginalConnStr string
	Database        string
	Scope           string
}

func TestMain(m *testing.M) {
	var connStr = envFlagString("CBCONNSTR", "connstr", "",
		"Connection string to run tests with")

	var user = envFlagString("CBUSER", "user", "Administrator",
		"The username to use to authenticate when using a real server")

	var password = envFlagString("CBPASS", "pass", "password",
		"The password to use to authenticate when using a real server")

	var database = envFlagString("CBDB", "database", "default",
		"The database to use to authenticate when using a real server")

	var scope = envFlagString("CBSCOPE", "scope", "test",
		"The scope to use to authenticate when using a real server")

	var disableLogger = envFlagBool("CBNOLOG", "disable-logger", false,
		"Whether to disable logging")

	flag.Parse()

	if *connStr == "" {
		panic("connstr cannot be empty")
	}

	if !*disableLogger {
		// Set up our special logger which logs the log level count
		globalTestLogger = createTestLogger()
		cbanalytics.SetLogger(globalTestLogger)
	}

	TestOpts.OriginalConnStr = *connStr
	TestOpts.Username = *user
	TestOpts.Password = *password
	TestOpts.Database = *database
	TestOpts.Scope = *scope

	leakcheck.EnableAll()

	setupColumnar()

	result := m.Run()

	if globalTestLogger != nil {
		log.Printf("Log Messages Emitted:")

		var preLogTotal uint64

		for i := 0; i < int(cbanalytics.LogMaxVerbosity); i++ {
			count := atomic.LoadUint64(&globalTestLogger.LogCount[i])
			preLogTotal += count
			log.Printf("  (%s): %d", logLevelToString(cbanalytics.LogLevel(i)), count)
		}

		abnormalLogCount := atomic.LoadUint64(&globalTestLogger.LogCount[cbanalytics.LogError]) + atomic.LoadUint64(&globalTestLogger.LogCount[cbanalytics.LogWarn])
		if abnormalLogCount > 0 {
			log.Printf("Detected unexpected logging, failing")

			result = 1
		}

		time.Sleep(1 * time.Second)

		log.Printf("Post sleep log Messages Emitted:")

		var postLogTotal uint64

		for i := 0; i < int(cbanalytics.LogMaxVerbosity); i++ {
			count := atomic.LoadUint64(&globalTestLogger.LogCount[i])
			postLogTotal += count
			log.Printf("  (%s): %d", logLevelToString(cbanalytics.LogLevel(i)), count)
		}

		if preLogTotal != postLogTotal {
			log.Printf("Detected unexpected logging after agent closed, failing")

			result = 1
		}
	}

	if !leakcheck.ReportAll() {
		result = 1
	}

	os.Exit(result)
}

func setupColumnar() {
	cluster, err := cbanalytics.NewCluster(TestOpts.OriginalConnStr, cbanalytics.NewCredential(TestOpts.Username, TestOpts.Password), DefaultOptions())
	if err != nil {
		panic(err)
	}
	defer func(cluster *cbanalytics.Cluster) {
		err := cluster.Close()
		if err != nil {
			panic(err)
		}
	}(cluster)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err = cluster.ExecuteQuery(ctx, fmt.Sprintf("CREATE DATABASE `%s` IF NOT EXISTS", TestOpts.Database))
	if err != nil {
		panic(err)
	}

	_, err = cluster.ExecuteQuery(ctx, fmt.Sprintf("CREATE SCOPE `%s`.`%s` IF NOT EXISTS", TestOpts.Database, TestOpts.Scope))
	if err != nil {
		panic(err)
	}
}

func envFlagBool(envName, name string, value bool, usage string) *bool {
	envValue := os.Getenv(envName)
	if envValue != "" {
		switch {
		case envValue == "0":
			value = false
		case strings.ToLower(envValue) == "false":
			value = false
		default:
			value = true
		}
	}

	return flag.Bool(name, value, usage)
}

func envFlagString(envName, name, value, usage string) *string {
	envValue := os.Getenv(envName)
	if envValue != "" {
		value = envValue
	}

	return flag.String(name, value, usage)
}

func DefaultOptions() *cbanalytics.ClusterOptions {
	return cbanalytics.NewClusterOptions().SetSecurityOptions(cbanalytics.NewSecurityOptions().SetDisableServerCertificateVerification(true))
}

var globalTestLogger *testLogger

type testLogger struct {
	Parent           cbanalytics.Logger
	LogCount         []uint64
	suppressWarnings uint32
}

func (logger *testLogger) Log(level cbanalytics.LogLevel, offset int, format string, v ...interface{}) error {
	if level >= 0 && level < cbanalytics.LogMaxVerbosity {
		if atomic.LoadUint32(&logger.suppressWarnings) == 1 && level == cbanalytics.LogWarn {
			level = cbanalytics.LogInfo
		}
		// We suppress this warning as this is ok.
		if strings.Contains(format, "server certificate verification is disabled") {
			level = cbanalytics.LogInfo
		}

		atomic.AddUint64(&logger.LogCount[level], 1)
	}

	return logger.Parent.Log(level, offset+1, fmt.Sprintf("[%s] ", logLevelToString(level))+format, v...) // nolint:wrapcheck
}

func (logger *testLogger) SuppressWarnings(suppress bool) {
	if suppress {
		atomic.StoreUint32(&logger.suppressWarnings, 1)
	} else {
		atomic.StoreUint32(&logger.suppressWarnings, 0)
	}
}

func createTestLogger() *testLogger {
	return &testLogger{
		Parent:           cbanalytics.VerboseStdioLogger(),
		LogCount:         make([]uint64, cbanalytics.LogMaxVerbosity),
		suppressWarnings: 0,
	}
}

func logLevelToString(level cbanalytics.LogLevel) string {
	switch level {
	case cbanalytics.LogError:
		return "error"
	case cbanalytics.LogWarn:
		return "warn"
	case cbanalytics.LogInfo:
		return "info"
	case cbanalytics.LogDebug:
		return "debug"
	case cbanalytics.LogTrace:
		return "trace"
	case cbanalytics.LogSched:
		return "sched"
	}

	return fmt.Sprintf("unknown (%d)", level)
}
