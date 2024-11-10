package gorche

type TableModel interface {
	// TableName returns the name of the target table in the database.
	TableName() string

	// ColumnNames returns the name list of columns in the database table.
	ColumnNames() []string
}
