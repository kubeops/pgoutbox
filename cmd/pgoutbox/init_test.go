/*
Copyright AppsCode Inc. and Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"testing"

	"kubeops.dev/pgoutbox/apis"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

func TestConnString(t *testing.T) {
	tests := []struct {
		name string
		cfg  apis.DatabaseCfg
	}{
		{
			name: "plain values",
			cfg:  apis.DatabaseCfg{Name: "bb", User: "postgres", Password: "postgres"},
		},
		{
			name: "password with whitespace and quotes",
			cfg:  apis.DatabaseCfg{Name: "bb", User: "postgres", Password: `p@ss w'rd\x`},
		},
		{
			name: "empty password",
			cfg:  apis.DatabaseCfg{Name: "bb", User: "postgres"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// pgx parses the string with libpq's keyword/value rules, so a
			// round trip through it is what proves the quoting is right.
			cfg, err := pgconn.ParseConfig(connString(&tt.cfg, "db-0", 5432) + " replication=database")
			if !assert.NoError(t, err) {
				return
			}

			assert.Equal(t, "db-0", cfg.Host)
			assert.Equal(t, uint16(5432), cfg.Port)
			assert.Equal(t, tt.cfg.Name, cfg.Database)
			assert.Equal(t, tt.cfg.User, cfg.User)
			assert.Equal(t, tt.cfg.Password, cfg.Password)
			assert.Equal(t, "database", cfg.RuntimeParams["replication"])
		})
	}
}
