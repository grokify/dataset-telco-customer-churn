package mysql

import (
	"errors"
	"log/slog"

	"github.com/go-sql-driver/mysql"
)

var ErrSQLXClientMustBeSet = errors.New("sqlx.DB must be set")

const (
	ErrNoMySQLDuplicateEntry = uint16(1062)
	ErrNoMySQLDuplicateTable = uint16(1050)
)

func ErrorExcludeDuplicate(err error, duplicateErrorNo uint16) error {
	if err == nil {
		return nil
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == duplicateErrorNo {
		// Duplicate entry error — safe to ignore
		slog.Info("Skipping duplicate entry", "mysqlError", mysqlErr.Message)
		return nil
	} else {
		return err
	}
}
