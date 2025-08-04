package mysql

import (
	"errors"
	"log/slog"

	"github.com/go-sql-driver/mysql"
)

func ErrorExcludeDuplicateEntry(err error) error {
	if err == nil {
		return nil
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		// Duplicate entry error — safe to ignore
		slog.Info("Skipping duplicate entry", "mysqlError", mysqlErr.Message)
		return nil
	} else {
		return err
	}
}
