package sqlite

import (
	"database/sql"
	"fmt"

	"github.com/VarozXYZ/vernier/internal/safeerr"
)

// sanitizeDurableDiagnostics removes credentials written by older binaries.
// Table and column names are compile-time constants supplied by the store
// constructors, never external input.
func sanitizeDurableDiagnostics(
	db *sql.DB,
	table string,
	column string,
) error {
	rows, err := db.Query(fmt.Sprintf(
		"SELECT rowid, %s FROM %s WHERE %s <> ''",
		column,
		table,
		column,
	))
	if err != nil {
		return err
	}
	type diagnostic struct {
		rowID int64
		value string
	}
	var updates []diagnostic
	for rows.Next() {
		var row diagnostic
		if err := rows.Scan(&row.rowID, &row.value); err != nil {
			_ = rows.Close()
			return err
		}
		sanitized := safeerr.Sanitize(row.value)
		if sanitized != row.value {
			row.value = sanitized
			updates = append(updates, row)
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, update := range updates {
		if _, err := db.Exec(
			fmt.Sprintf("UPDATE %s SET %s=? WHERE rowid=?", table, column),
			update.value,
			update.rowID,
		); err != nil {
			return err
		}
	}
	return nil
}
