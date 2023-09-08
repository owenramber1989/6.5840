package logger

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	DClient  logTopic = "CLIENT"
	DCommit  logTopic = "COMMIT"
	DDrop    logTopic = "DROP"
	DError   logTopic = "ERROR"
	DInfo    logTopic = "INFO"
	DLeader  logTopic = "LEADER"
	DLog     logTopic = "LOG1"
	DLog2    logTopic = "LOG2"
	DPersist logTopic = "PERSIST"
	DSnap    logTopic = "SNAP"
	DTerm    logTopic = "TERM"
	DTest    logTopic = "TEST"
	DTimer   logTopic = "TIMRER"
	DTrace   logTopic = "TRACE"
	DVote    logTopic = "VOTE"
	DWarn    logTopic = "WARN"
	DDebug   logTopic = "DEBUG"
)

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	log.SetOutput(os.Stdout)
}

func Debug(topic logTopic, format string, a ...any) {
	if debugVerbosity >= 1 {
		current := time.Now()
		t := time.Since(debugStart).Microseconds()
		t /= 100

		prefix := fmt.Sprintf("%+v %8d %6v ", current.Format("2006-01-02 15:04:05.00000000"), t, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
