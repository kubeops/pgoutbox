# Making pgoutbox work behind pgpool

A walkthrough of this change: the bug, why it happened, and what each edit does.

## The symptom

pgoutbox started fine, then died about 60 seconds later:

```
INFO  slot already exists, LSN updated
ERROR failed to send heartbeat status  err="unable to send StandbyStatusUpdate: write failed: ... 10.43.11.67:9999: use of closed network connection"
ERROR service process failed           err="group: receive message: FATAL: unable to read data from DB node 0 (SQLSTATE XX000)"
```

Note the port: **9999** is pgpool, not PostgreSQL.

## The cause

A logical replication stream cannot pass through a connection pooler. Three
separate things were broken:

1. **pgpool doesn't understand replication connections.** It never implemented
   the `replication=database` startup option — it forwards the startup packet to
   *every* backend, standbys included — and it has no state machine for
   **CopyBoth**, the mode `START_REPLICATION` switches a connection into. So it
   waits for a `ReadyForQuery` that never arrives. `client_idle_limit: "60"` in
   the chart is what finally cut the connection at the 60-second mark.
2. **Read/write splitting broke the slot bookkeeping.** `SELECT restart_lsn FROM
   pg_replication_slots` is a plain read, so pgpool could answer it from a
   *standby*, where the slot state differs from the primary's.
3. **DDL was fanned out to read-only replicas.** `CREATE PUBLICATION` and
   `ALTER TABLE … REPLICA IDENTITY FULL` go to every backend, and standbys reject
   writes.

## The fix, in four ideas

### 1. Always talk to the primary directly (`internal/pgpool/`, `cmd/pgoutbox/init.go`)

Since a pooler can't carry the stream, pgoutbox now bypasses it. But you
shouldn't have to reconfigure anything, so it figures out where to go on its own:

```
connect to database.host
  └─ run "SHOW pool_nodes"
       ├─ error 42704 (unrecognized parameter) → this is plain PostgreSQL, use it
       └─ it worked                            → this is pgpool, so:
            pick the backend PostgreSQL itself calls primary → dial that node
```

Three details that are easy to get wrong:

- **The query must use the *simple* protocol.** pgpool intercepts `SHOW
  pool_nodes` itself, but only in simple query mode; in extended mode it forwards
  it to PostgreSQL, which can't parse it. Hence
  `pgx.QueryExecModeSimpleProtocol`.
- **Trust `pg_role`, not `role`.** `role` is pgpool's own belief and lags behind
  a failover. `pg_role` comes from `pg_is_in_recovery()` on the backend, so it's
  authoritative. `role` is only the fallback.
- **Columns are read by name, not position.** `pg_status` and `pg_role` only
  exist since pgpool 4.3, so positional indexing would break on older versions.

Resolution happens **inside** the connection retry loop, not once before it, so
nothing is cached and every restart re-resolves.

### 2. Verify it, don't trust it (`cmd/pgoutbox/init.go`)

Knowing *where* the primary is and knowing the node *is* the primary are two
different questions, and neither address source answers the second one reliably:
a plain host can point at a replica, and pgpool's `pg_role` comes from a
background poll, so it can name a backend that was already demoted. So after
connecting, pgoutbox asks the node directly:

```go
SELECT pg_is_in_recovery()   // true → this is a standby, don't proceed
```

A standby is a **retry, not a failure** — it returns `false, nil` into the same
`PollUntilContextTimeout` loop, so pgoutbox waits out a failover in progress
instead of latching onto the wrong node. This runs *before* the replication
connection is opened and before `CREATE PUBLICATION`, which would otherwise fail
on a read-only node with a much less obvious error.

### 3. Notice when the node stops being the primary (`internal/listener/`)

The check above runs at startup; this one runs while streaming. `IsAlive()` only
returns false once PostgreSQL has *closed* the socket. A demoted node holding the
connection open, or a black-holed network path, would leave pgoutbox looking
healthy while publishing nothing — the worst failure mode for an outbox. So
`checkConnection` asks the same question on a ticker:

```go
SELECT pg_is_in_recovery()   // true → we're on a standby now, exit
```

5-second timeout, and 3 consecutive failures tolerated so a briefly busy database
doesn't restart the pod. `refreshConnection` drops from 30s to **15s**, which
halves worst-case detection time; the query is a cheap, lock-free function call,
so 4/minute costs nothing.

One limit worth knowing: the 3-failure tolerance only absorbs errors the *server
answers with*. A check that hits the 5-second timeout makes pgx close the
connection, so the `IsAlive()` check on the next tick fails regardless of the
count. Surviving that in-process would mean reconnecting rather than exiting,
which is a larger change.

**This exposed a real bug.** The two goroutines used a plain `errgroup.Group`, so
`checkConnection` returning an error could not stop `Stream`, which sits blocked
in `ReceiveMessage` — and `group.Wait()` waits for *all* goroutines. The whole
connection-check mechanism was a no-op that could hang forever. Switching to
`errgroup.WithContext` makes the first failure cancel the other goroutine.

### 4. Fail visibly (`cmd/pgoutbox/main.go`)

`Process` failing used to be logged and then `main` returned **exit 0** — a
crashed listener looked like a clean shutdown. Now it returns the error and exits
**1**, so restarts show up as failures.

## Smaller fixes picked up on the way

| What | Why it mattered |
|---|---|
| **Standby status updates are serialized behind `sendMu`** | They are sent from two goroutines — the stream, on ack and on a requested reply, and the heartbeat. pglogrepl writes them straight to the connection's frontend *without* taking pgconn's lock, so two senders raced on the write buffer and could interleave two `CopyData` frames into one malformed message |
| **The heartbeat can no longer outlive `Stream`** | It shares the replication connection, but nothing tied its lifetime to the stream's. When `Stream` returned on an error the heartbeat kept writing while the caller closed that connection underneath it. `Stream` now cancels it and waits |
| **`Stop()` moved to after `group.Wait()`** | `checkConnection` used to close both connections from its `ctx.Done()` branch, i.e. while `Stream` and the heartbeat were still using them |
| **A cancelled context is no longer an error** | pgconn aborts the blocked read by setting a socket deadline, so a normal SIGTERM surfaced as an I/O timeout, became a `Process` error, and — now that `main` exits non-zero — made every rolling update look like a crash |
| **`connectAttemptTimeout` per connection attempt** | pgx sets *no* dial timeout of its own (`makeDefaultDialer` returns a bare `&net.Dialer{}`), so one unreachable address blocked on the kernel's SYN retries and ate most of the 10-minute budget without the 5s retry interval ever applying |
| **NULL `restart_lsn` is handled** | Scanning NULL into a `string` failed, and this is reachable exactly on the failover path: a slot synchronized by `sync_replication_slots` has no restart LSN until the first sync finishes. Now `ErrSlotNotReady`, tested in SQL so no NULL is ever scanned |
| Connection string values are single-quoted | `password=hunter 2` silently parsed as password `hunter` plus a junk keyword. `init_test.go` round-trips through `pgconn.ParseConfig` — pgx's own parser — to prove the escaping holds |
| The SQL connection is closed when the replication connect fails | It leaked one connection per retry |
| `PollUntilContextTimeout` gets the real `ctx` | It was `context.TODO()`, so SIGTERM couldn't abort a 10-minute startup wait |

## What happens on failover now

1. Connections to the old primary break, ending the stream — or if they stay
   open, the `pg_is_in_recovery()` check catches it within `refreshConnection`.
2. pgoutbox exits non-zero; Kubernetes restarts it.
3. Startup re-resolves the primary and verifies it, retrying every 5s for up to
   10 minutes. So a promotion still in flight is waited out inside one process
   rather than becoming a crash loop.

**Resuming without a gap needs `listener.failover: true`** plus PostgreSQL 17's
`sync_replication_slots=on` (already set in the chart's `db-config.yaml`), which
keeps the slot in step on the standbys. Without it the new primary has no slot, a
fresh one is created at the current LSN, and anything committed before the
promotion but not yet published is lost.

## Which host should I configure?

Both work. `database.host` accepts either a PostgreSQL endpoint or a pgpool one,
and the difference is only how the primary's address is found:

| `database.host` | How the primary is found | Note |
|---|---|---|
| `ace-db.<ns>.svc:5432` (primary Service) | used as given | Endpoints are maintained by the KubeDB operator, so they follow a promotion directly. Preferred. |
| `ace-pgpool.<ns>.svc:9999` | `SHOW pool_nodes` | One extra hop, and pgpool's `pg_role` is refreshed by a background poll, so just after a failover it can still name the old primary — the recovery check catches that and retries. |

The one assumption worth checking on the pgpool path: the hostnames pgpool
reports come from *its own* `pgpool.conf`, so they have to be dialable from the
pgoutbox pod. Verify with:

```
kubectl exec -n <ns> ace-pgpool-0 -- \
  env PGPASSWORD=... psql -h localhost -p 9999 -U postgres -d bb -c 'show pool_nodes'
```
