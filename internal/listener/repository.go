package listener

import (
	"context"
	"errors"
	"fmt"
	"strings"

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

// ErrSlotNotReady reports a slot that exists but has no restart LSN yet. A slot
// synchronized from a primary by sync_replication_slots looks like this until the
// first sync completes, which is exactly the state a just promoted standby can be
// found in. There is nothing to resume from and nothing to create, so the caller
// has to wait rather than treat it as either.
var ErrSlotNotReady = errors.New("replication slot has no restart lsn yet")

// GetSlotLSN returns the value of the last offset for a specific slot. It
// returns an empty string when the slot does not exist.
func (r RepositoryImpl) GetSlotLSN(ctx context.Context, slotName string) (string, error) {
	var (
		lsnIsNull     bool
		restartLSNStr string
	)

	// restart_lsn is nullable, so it is tested and defaulted in SQL: scanning a
	// NULL into a string fails, which would turn a recoverable wait into a
	// startup error about the wrong thing.
	err := r.conn.QueryRow(ctx,
		"SELECT restart_lsn IS NULL, coalesce(restart_lsn::text, '') FROM pg_replication_slots WHERE slot_name=$1;",
		slotName).Scan(&lsnIsNull, &restartLSNStr)

	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}

	if err != nil {
		return "", err
	}

	if lsnIsNull {
		return "", ErrSlotNotReady
	}

	return restartLSNStr, nil
}

// EnsurePublication makes the named publication cover exactly the given
// tables, replacing whatever definition it had, so that pgoutput sends only
// their changes. With no tables the publication is empty.
//
// The drop and the create share one transaction. Logical decoding looks the
// publication up by name in the catalog as of each decoded change, so dropping
// it separately leaves a stretch of WAL in which it does not exist. Up to
// PostgreSQL 17 decoding that stretch fails on every restart, leaving the slot
// stuck, and from 18 on the changes in it are skipped without being published.
func (r RepositoryImpl) EnsurePublication(ctx context.Context, name string, tables []string) error {
	return pgx.BeginFunc(ctx, r.conn, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pgx.Identifier{name}.Sanitize()); err != nil {
			return fmt.Errorf("drop publication: %w", err)
		}

		if _, err := tx.Exec(ctx, createPublicationSQL(name, tables)); err != nil {
			return fmt.Errorf("create publication: %w", err)
		}

		return nil
	})
}

// createPublicationSQL builds the statement creating the publication for the
// given tables. The names are quoted, so each one has to be the exact relation
// name, which is also what the filter matches events against.
func createPublicationSQL(name string, tables []string) string {
	query := "CREATE PUBLICATION " + pgx.Identifier{name}.Sanitize()
	if len(tables) == 0 {
		return query
	}

	quoted := make([]string, len(tables))
	for i, table := range tables {
		quoted[i] = pgx.Identifier{table}.Sanitize()
	}

	return query + " FOR TABLE " + strings.Join(quoted, ", ")
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

// IsInRecovery reports whether the connected node is a standby. A node that was
// demoted by a failover answers true, and it has no WAL to stream. The query
// doubles as an active connection check: an open but dead socket makes it fail
// or time out, which IsAlive cannot see.
func (r RepositoryImpl) IsInRecovery(ctx context.Context) (bool, error) {
	var inRecovery bool

	if err := r.conn.QueryRow(ctx, "SELECT pg_is_in_recovery();").Scan(&inRecovery); err != nil {
		return false, fmt.Errorf("query recovery state: %w", err)
	}

	return inRecovery, nil
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
