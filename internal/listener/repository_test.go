package listener

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreatePublicationSQL(t *testing.T) {
	tests := []struct {
		name   string
		tables []string
		want   string
	}{
		{
			name: "no tables",
			want: `CREATE PUBLICATION "pgoutbox"`,
		},
		{
			name:   "tables",
			tables: []string{"ac_team", "ac_user"},
			want:   `CREATE PUBLICATION "pgoutbox" FOR TABLE "ac_team", "ac_user"`,
		},
		{
			// Quoted, a name keeps its case and cannot break out of the statement.
			name:   "names are quoted identifiers",
			tables: []string{"Users", `odd"name`},
			want:   `CREATE PUBLICATION "pgoutbox" FOR TABLE "Users", "odd""name"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, createPublicationSQL("pgoutbox", tt.tables))
		})
	}
}
