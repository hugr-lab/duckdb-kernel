package engine

import (
	"testing"
)

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"my_table", `"my_table"`},
		{`Robert"; DROP TABLE students--`, `"Robert""; DROP TABLE students--"`},
		{"", `""`},
		{`has"quotes`, `"has""quotes"`},
		{"normal", `"normal"`},
	}
	for _, tt := range tests {
		got := quoteIdentifier(tt.input)
		if got != tt.want {
			t.Errorf("quoteIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestQualifiedTable(t *testing.T) {
	tests := []struct {
		database, schema, table string
		want                    string
	}{
		{"", "", "t", `"t"`},
		{"", "main", "t", `"main"."t"`},
		{"db", "main", "t", `"db"."main"."t"`},
		{"db", "", "t", `"db"."t"`},
		{"", "", `evil";DROP TABLE x--`, `"evil"";DROP TABLE x--"`},
	}
	for _, tt := range tests {
		got := qualifiedTable(tt.database, tt.schema, tt.table)
		if got != tt.want {
			t.Errorf("qualifiedTable(%q,%q,%q) = %q, want %q",
				tt.database, tt.schema, tt.table, got, tt.want)
		}
	}
}

func TestEscapeSingleQuote(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"hello", "hello"},
		{"it's", "it''s"},
		{"a'b'c", "a''b''c"},
	}
	for _, tt := range tests {
		got := escapeSingleQuote(tt.input)
		if got != tt.want {
			t.Errorf("escapeSingleQuote(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
