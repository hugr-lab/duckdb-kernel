package renderer

import (
	"bytes"
	"strings"

	"github.com/olekukonko/tablewriter"
)

// RenderTable formats columns and rows as a plain text ASCII table.
func RenderTable(columns []string, rows [][]string) string {
	buf := &bytes.Buffer{}
	tw := tablewriter.NewWriter(buf)

	// Set header
	headerRow := make([]any, len(columns))
	for i, col := range columns {
		headerRow[i] = col
	}
	tw.Header(headerRow...)

	// Append rows
	for _, row := range rows {
		rowAny := make([]any, len(row))
		for i, cell := range row {
			rowAny[i] = cell
		}
		tw.Append(rowAny...)
	}

	tw.Render()
	return strings.TrimSpace(buf.String())
}

// RenderText returns a plain text message (for meta commands, etc.).
func RenderText(text string) string {
	return text
}
