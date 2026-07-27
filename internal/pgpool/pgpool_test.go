package pgpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelectPrimary(t *testing.T) {
	tests := []struct {
		name     string
		nodes    []Node
		wantAddr string
		wantErr  string
	}{
		{
			name: "pg_role wins over stale pgpool role",
			nodes: []Node{
				{ID: "0", Host: "db-0", Port: 5432, Status: "up", Role: "primary", PgRole: "standby"},
				{ID: "1", Host: "db-1", Port: 5432, Status: "up", Role: "standby", PgRole: "primary"},
			},
			wantAddr: "db-1:5432",
		},
		{
			name: "falls back to pgpool role when pg_role is unknown",
			nodes: []Node{
				{ID: "0", Host: "db-0", Port: 5432, Status: "up", Role: "standby", PgRole: "unknown"},
				{ID: "1", Host: "db-1", Port: 5433, Status: "up", Role: "primary", PgRole: "unknown"},
			},
			wantAddr: "db-1:5433",
		},
		{
			name: "waiting node counts as up",
			nodes: []Node{
				{ID: "0", Host: "db-0", Port: 5432, Status: "waiting", Role: "primary", PgRole: "primary"},
			},
			wantAddr: "db-0:5432",
		},
		{
			name: "main is the primary in non streaming replication modes",
			nodes: []Node{
				{ID: "0", Host: "db-0", Port: 5432, Status: "up", Role: "replica", PgRole: "unknown"},
				{ID: "1", Host: "db-1", Port: 5432, Status: "up", Role: "main", PgRole: "unknown"},
			},
			wantAddr: "db-1:5432",
		},
		{
			name: "skips a primary pgpool marks as down",
			nodes: []Node{
				{ID: "0", Host: "db-0", Port: 5432, Status: "down", Role: "primary", PgRole: "primary"},
				{ID: "1", Host: "db-1", Port: 5432, Status: "up", Role: "standby", PgRole: "primary"},
			},
			wantAddr: "db-1:5432",
		},
		{
			name: "no primary at all",
			nodes: []Node{
				{ID: "0", Host: "db-0", Port: 5432, Status: "up", Role: "standby", PgRole: "standby"},
			},
			wantErr: ErrNoPrimary.Error(),
		},
		{
			name:    "empty report",
			nodes:   nil,
			wantErr: ErrNoPrimary.Error(),
		},
		{
			name: "unix socket backend is not dialable",
			nodes: []Node{
				{ID: "0", Host: "/var/run/postgresql", Port: 5432, Status: "up", Role: "primary", PgRole: "primary"},
			},
			wantErr: "point database.host at the PostgreSQL primary",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := selectPrimary(tt.nodes)

			if tt.wantErr != "" {
				if assert.Error(t, err) {
					assert.Contains(t, err.Error(), tt.wantErr)
				}

				return
			}

			if assert.NoError(t, err) {
				assert.Equal(t, tt.wantAddr, node.Addr())
			}
		})
	}
}

func TestField(t *testing.T) {
	column := map[string]int{"node_id": 0, "hostname": 1, "port": 2}
	values := [][]byte{[]byte("0"), []byte("db-0"), []byte("5432")}

	assert.Equal(t, "db-0", field(values, column, "hostname"))
	// pg_role does not exist before pgpool 4.3.
	assert.Empty(t, field(values, column, "pg_role"))
	// A column pgpool announced but did not send a value for.
	assert.Empty(t, field(values, map[string]int{"role": 7}, "role"))
}
