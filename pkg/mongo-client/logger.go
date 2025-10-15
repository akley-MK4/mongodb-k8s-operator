package mongoclient

type ILogger interface {
	InfoF(format string, v ...any)
	WarningF(format string, v ...any)
}

type Logger struct{}

func (t *Logger) InfoF(format string, v ...any) {}

func (t *Logger) WarningF(format string, v ...any) {}

func (t *Logger) ErrorF(format string, v ...any) {}

var (
	logger ILogger = &Logger{}
)

func SetLogger(logInst ILogger) {
	logger = logInst
}
