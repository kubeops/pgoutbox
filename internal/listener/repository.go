package listener

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// RepositoryImpl service repository.
type RepositoryImpl struct {
	conn *pgx.Conn
}

// NewRepository returns a new instance of the repository.
func NewRepository(conn *pgx.Conn) *RepositoryImpl {
	return &RepositoryImpl{conn: conn}
}

// GetSlotLSN returns the value of the last offset for a specific slot.
func (r RepositoryImpl) GetSlotLSN(ctx context.Context, slotName string) (string, error) {
	var restartLSNStr string

	err := r.conn.QueryRow(ctx, "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name=$1;", slotName).
		Scan(&restartLSNStr)

	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}

	return restartLSNStr, err
}

// CreatePublication create publication fo all.
func (r RepositoryImpl) CreatePublication(ctx context.Context, name string) error {
	if _, err := r.conn.Exec(ctx, `CREATE PUBLICATION "`+name+`" FOR ALL TABLES`); err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

// CreateFailoverSlot creates a logical replication slot with the failover
// property enabled so PostgreSQL can synchronize it to hot standbys (requires
// PostgreSQL 17+). It returns the consistent point LSN of the new slot.
//
// The slot is created through the SQL function rather than the replication
// protocol because the CREATE_REPLICATION_SLOT command exposed by pglogrepl
// cannot express the FAILOVER option.
func (r RepositoryImpl) CreateFailoverSlot(ctx context.Context, slotName string) (string, error) {
	var lsn string

	// args: slot_name, plugin, temporary=false, twophase=false, failover=true
	err := r.conn.QueryRow(ctx,
		"SELECT lsn FROM pg_create_logical_replication_slot($1, 'pgoutput', false, false, true);",
		slotName).Scan(&lsn)
	if err != nil {
		return "", fmt.Errorf("create failover slot: %w", err)
	}

	return lsn, nil
}

// IsAlive check database connection problems.
func (r RepositoryImpl) IsAlive() bool {
	return !r.conn.IsClosed()
}

// Close database connection.
func (r RepositoryImpl) Close(ctx context.Context) error {
	return r.conn.Close(ctx)
}

// IsReplicationActive returns true if the replication slot is already active, false otherwise.
func (r RepositoryImpl) IsReplicationActive(ctx context.Context, slotName string) (bool, error) {
	var activePID int

	err := r.conn.QueryRow(ctx, "SELECT active_pid FROM pg_replication_slots WHERE slot_name=$1 AND active=true;", slotName).
		Scan(&activePID)

	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}

	return true, err
}
