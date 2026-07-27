// Package pgpool resolves the PostgreSQL primary that sits behind a pgpool-II
// endpoint.
//
// pgpool-II cannot proxy a logical replication stream. It has no handling for
// the `replication=database` startup option — the startup packet is forwarded
// verbatim to every backend, standbys included — and no state machine for the
// CopyBoth mode that START_REPLICATION switches the connection into. The stream
// therefore stalls and is eventually torn down by pgpool. Slot bookkeeping is
// not safe through pgpool either: SELECTs against pg_replication_slots are load
// balanced onto standbys, where the slot state differs from the primary's.
//
// pgoutbox uses pgpool only to ask which backend is currently the primary, and
// then talks to that node directly.
package pgpool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// showPoolNodes is answered by pgpool itself. It has to be sent as a simple
// query: pgpool does not intercept SQL type commands in extended query mode and
// forwards them to PostgreSQL, which fails to parse them.
const showPoolNodes = "SHOW pool_nodes"

// SQLSTATEs a server that does not know `SHOW pool_nodes` answers with.
// PostgreSQL reports an unrecognized configuration parameter as 42704.
const (
	sqlStateUndefinedObject = "42704"
	sqlStateSyntaxError     = "42601"
)

var (
	// ErrNotPgpool reports that the endpoint is a plain PostgreSQL server, i.e.
	// it does not know the pgpool specific `SHOW pool_nodes` command.
	ErrNotPgpool = errors.New("endpoint is not pgpool")

	// ErrNoPrimary reports that none of pgpool's backends is up and writable.
	ErrNoPrimary = errors.New("pgpool reports no primary backend")
)

// Node is one backend entry of pgpool's `SHOW pool_nodes` report.
type Node struct {
	ID   string
	Host string
	Port uint16
	// Status is pgpool's own view of the backend: up, waiting (up, but without a
	// pooled connection yet), down, unused or quarantine.
	Status string
	// Role is pgpool's own view of the backend: primary or standby in streaming
	// replication mode, main or replica in the other clustering modes, unknown
	// otherwise. It can lag behind an actual failover.
	Role string
	// PgStatus and PgRole are queried from PostgreSQL itself and are therefore
	// authoritative. They exist since pgpool 4.3 and hold "unknown" when
	// pgpool's streaming replication check is disabled.
	PgStatus string
	PgRole   string
}

// Addr returns the node address in host:port form.
func (n Node) Addr() string {
	return net.JoinHostPort(n.Host, strconv.Itoa(int(n.Port)))
}

// Primary asks pgpool which of its backends is currently the writable one. conn
// has to be connected to pgpool itself, not to PostgreSQL. It returns
// ErrNotPgpool when conn turns out to talk to a plain PostgreSQL server.
func Primary(ctx context.Context, conn *pgx.Conn) (Node, error) {
	nodes, err := queryNodes(ctx, conn)
	if err != nil {
		return Node{}, err
	}

	return selectPrimary(nodes)
}

// queryNodes reads the backends pgpool is configured with.
func queryNodes(ctx context.Context, conn *pgx.Conn) ([]Node, error) {
	rows, err := conn.Query(ctx, showPoolNodes, pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		return nil, wrapQueryErr(err)
	}
	defer rows.Close()

	// The report grew columns over pgpool releases — pg_status and pg_role only
	// exist since 4.3 — so address the values by name instead of by position.
	column := make(map[string]int, len(rows.FieldDescriptions()))
	for i, fd := range rows.FieldDescriptions() {
		column[fd.Name] = i
	}

	var nodes []Node

	for rows.Next() {
		// The simple query protocol returns every column as text.
		values := rows.RawValues()

		node := Node{
			ID:       field(values, column, "node_id"),
			Host:     field(values, column, "hostname"),
			Status:   field(values, column, "status"),
			Role:     field(values, column, "role"),
			PgStatus: field(values, column, "pg_status"),
			PgRole:   field(values, column, "pg_role"),
		}

		port, err := strconv.ParseUint(field(values, column, "port"), 10, 16)
		if err != nil {
			return nil, fmt.Errorf("parse port of node %q: %w", node.ID, err)
		}

		node.Port = uint16(port)
		nodes = append(nodes, node)
	}

	if err := rows.Err(); err != nil {
		return nil, wrapQueryErr(err)
	}

	return nodes, nil
}

// wrapQueryErr translates the rejection of `SHOW pool_nodes` by a server that
// does not know the command into ErrNotPgpool.
func wrapQueryErr(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) &&
		(pgErr.Code == sqlStateUndefinedObject || pgErr.Code == sqlStateSyntaxError) {
		return ErrNotPgpool
	}

	return fmt.Errorf("%s: %w", showPoolNodes, err)
}

func field(values [][]byte, column map[string]int, name string) string {
	i, ok := column[name]
	if !ok || i >= len(values) {
		return ""
	}

	return string(values[i])
}

// selectPrimary picks the backend to send writes and replication commands to.
func selectPrimary(nodes []Node) (Node, error) {
	if len(nodes) == 0 {
		return Node{}, ErrNoPrimary
	}

	// pg_role comes from PostgreSQL itself, so trust it over pgpool's own view,
	// which can still name the demoted node after a failover.
	for _, roleOf := range []func(Node) string{
		func(n Node) string { return n.PgRole },
		func(n Node) string { return n.Role },
	} {
		for _, node := range nodes {
			if !isPrimaryRole(roleOf(node)) || !isUsable(node) {
				continue
			}

			if err := validateAddr(node); err != nil {
				return Node{}, err
			}

			return node, nil
		}
	}

	return Node{}, ErrNoPrimary
}

// isPrimaryRole reports whether role names the writable backend. Streaming
// replication mode calls it primary, the other clustering modes call it main.
func isPrimaryRole(role string) bool {
	return role == "primary" || role == "main"
}

// isUsable reports whether pgpool considers the backend reachable. A waiting
// node is up, it just has no pooled connection yet.
func isUsable(node Node) bool {
	switch node.Status {
	case "up", "waiting":
		return true
	default:
		return false
	}
}

// validateAddr rejects a backend pgoutbox cannot dial. pgpool talks to a local
// PostgreSQL over a unix socket when backend_hostname is empty or a directory
// path, which is unreachable from another host.
func validateAddr(node Node) error {
	if node.Host == "" || strings.HasPrefix(node.Host, "/") {
		return fmt.Errorf(
			"pgpool reaches the primary over the unix socket %q, which pgoutbox cannot dial: "+
				"point database.host at the PostgreSQL primary instead of at pgpool",
			node.Host,
		)
	}

	return nil
}
