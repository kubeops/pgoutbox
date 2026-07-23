package listener

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"kubeops.dev/pgoutbox/apis"
	tx "kubeops.dev/pgoutbox/internal/listener/transaction"

	"github.com/jackc/pglogrepl"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
	"kubeops.dev/pgoutbox/internal/telemetry"
)

// Logical decoding plugin.
const pgOutputPlugin = "pgoutput"

type eventPublisher interface {
	Publish(context.Context, string, *apis.Event) error
}

type parser interface {
	ParseWalMessage([]byte, *tx.WAL) error
}

type replication interface {
	CreateReplicationSlot(ctx context.Context, slotName, outputPlugin string) (pglogrepl.CreateReplicationSlotResult, error)
	DropReplicationSlot(ctx context.Context, slotName string) error
	StartReplication(ctx context.Context, slotName string, startLsn pglogrepl.LSN, options pglogrepl.StartReplicationOptions) error
	ReceiveMessage(ctx context.Context) ([]byte, error)
	SendStandbyStatusUpdate(ctx context.Context, status pglogrepl.StandbyStatusUpdate) error
	IsAlive() bool
	Close() error
}

type repository interface {
	CreatePublication(ctx context.Context, name string) error
	CreateFailoverSlot(ctx context.Context, slotName string) (string, error)
	GetSlotLSN(ctx context.Context, slotName string) (string, error)
	IsReplicationActive(ctx context.Context, slotName string) (bool, error)
	IsAlive() bool
	Close(ctx context.Context) error
}

type monitor interface {
	IncPublishedEvents(ctx context.Context, subject, table string)
	IncFilterSkippedEvents(ctx context.Context, table string)
	IncProblematicEvents(ctx context.Context, kind string)
	RecordProcessingDuration(ctx context.Context, seconds float64)
	RecordPublishDuration(ctx context.Context, seconds float64, subject string)
	RecordLSN(ctx context.Context, lsn int64)
}

// Listener main service struct.
type Listener struct {
	cfg        *apis.Config
	log        *slog.Logger
	monitor    monitor
	mu         sync.RWMutex
	publisher  eventPublisher
	replicator replication
	repository repository
	parser     parser
	lsn        pglogrepl.LSN
	isAlive    atomic.Bool
}

var (
	errReplConnectionIsLost = errors.New("replication connection to postgres is lost")
	errConnectionIsLost     = errors.New("db connection to postgres is lost")
	errReplDidNotStart      = errors.New("replication did not start")
)

// NewWalListener create and initialize new service instance.
func NewWalListener(
	cfg *apis.Config,
	log *slog.Logger,
	repo repository,
	repl replication,
	pub eventPublisher,
	parser parser,
	monitor monitor,
) *Listener {
	return &Listener{
		log:        log,
		monitor:    monitor,
		cfg:        cfg,
		publisher:  pub,
		repository: repo,
		replicator: repl,
		parser:     parser,
	}
}

// InitHandlers init web handlers for liveness and readiness k8s probes.
func (l *Listener) InitHandlers(ctx context.Context) {
	const defaultTimeout = 500 * time.Millisecond

	if l.cfg.Listener.ServerPort == 0 {
		l.log.Debug("web server port for probes not specified, skip")
		return
	}

	handler := http.NewServeMux()
	handler.HandleFunc("GET /healthz", l.liveness)
	handler.HandleFunc("GET /ready", l.readiness)

	if l.cfg.Telemetry == nil || l.cfg.Telemetry.Enabled {
		handler.Handle("GET /metrics", promhttp.Handler())
	}

	addr := ":" + strconv.Itoa(l.cfg.Listener.ServerPort)
	srv := http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  defaultTimeout,
		WriteTimeout: defaultTimeout,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			l.log.Error("error starting http listener", "err", err)
		}
	}()

	l.log.Debug("web handlers were initialised", slog.String("addr", addr))

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		l.log.Error("http server shutdown error", "err", err)
	}

	if err := telemetry.Shutdown(shutdownCtx); err != nil {
		l.log.Error("telemetry shutdown error", "err", err)
	}
}

const contentTypeTextPlain = "text/plain"

func (l *Listener) liveness(w http.ResponseWriter, _ *http.Request) {
	var (
		respCode = http.StatusOK
		resp     = []byte(`ok`)
	)

	w.Header().Set("Content-Type", contentTypeTextPlain)

	if !l.replicator.IsAlive() || !l.repository.IsAlive() || !l.isAlive.Load() {
		resp = []byte("failed")
		respCode = http.StatusInternalServerError

		l.log.Warn("liveness probe failed")
	} else {
		l.log.Debug("liveness probe successful")
	}

	w.WriteHeader(respCode)

	if _, err := w.Write(resp); err != nil {
		l.log.Error("liveness: error writing response", "err", err)
	}
}

func (l *Listener) readiness(w http.ResponseWriter, _ *http.Request) {
	var (
		respCode = http.StatusOK
		resp     = []byte(`ok`)
	)

	w.Header().Set("Content-Type", contentTypeTextPlain)

	if !l.isAlive.Load() {
		resp = []byte("failed")
		respCode = http.StatusInternalServerError

		l.log.Warn("readiness probe failed")
	} else {
		l.log.Debug("readiness probe successful")
	}

	w.WriteHeader(respCode)

	if _, err := w.Write(resp); err != nil {
		l.log.Error("readiness: error writing response", "err", err)
	}
}

// Process is the main service entry point.
func (l *Listener) Process(ctx context.Context) error {
	logger := l.log.With("slot_name", l.cfg.Listener.SlotName)

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	logger.Info("service was started")

	if err := l.repository.CreatePublication(ctx, publicationName); err != nil {
		logger.Warn("publication creation was skipped", "err", err)
	}

	slotIsExists, err := l.slotIsExists(ctx)
	if err != nil {
		return fmt.Errorf("slot is exists: %w", err)
	}

	if !slotIsExists {
		lsn, err := l.createSlot(ctx)
		if err != nil {
			return err
		}

		l.setLSN(ctx, lsn)

		logger.Info("new slot was created", slog.String("slot", l.cfg.Listener.SlotName))
	} else {
		logger.Info("slot already exists, LSN updated")
	}

	if replicationActive, err := l.repository.IsReplicationActive(ctx, l.cfg.Listener.SlotName); err != nil || replicationActive {
		l.log.Error(
			"replication seems to already be alive or unable to check it",
			slog.String("slot", l.cfg.Listener.SlotName),
		)

		return errReplDidNotStart
	}

	group := new(errgroup.Group)

	group.Go(func() error {
		return l.Stream(ctx)
	})
	group.Go(func() error {
		return l.checkConnection(ctx)
	})

	if err = group.Wait(); err != nil {
		return fmt.Errorf("group: %w", err)
	}

	return nil
}

// checkConnection periodically checks connections.
func (l *Listener) checkConnection(ctx context.Context) error {
	refresh := time.NewTicker(l.cfg.Listener.RefreshConnection)
	defer refresh.Stop()

	for {
		select {
		case <-refresh.C:
			if !l.replicator.IsAlive() {
				return fmt.Errorf("replicator: %w", errReplConnectionIsLost)
			}

			if !l.repository.IsAlive() {
				return fmt.Errorf("repository: %w", errConnectionIsLost)
			}
		case <-ctx.Done():
			l.log.Debug("check connection: context was canceled")

			if err := l.Stop(); err != nil {
				l.log.Error("failed to stop service", "err", err)
			}

			return nil
		}
	}
}

// createSlot creates the logical replication slot and returns its consistent
// point LSN. When failover is enabled the slot is created through the SQL function
func (l *Listener) createSlot(ctx context.Context) (pglogrepl.LSN, error) {
	if l.cfg.Listener.Failover {
		lsnStr, err := l.repository.CreateFailoverSlot(ctx, l.cfg.Listener.SlotName)
		if err != nil {
			return 0, fmt.Errorf("create failover replication slot: %w", err)
		}

		return pglogrepl.ParseLSN(lsnStr)
	}

	result, err := l.replicator.CreateReplicationSlot(ctx, l.cfg.Listener.SlotName, pgOutputPlugin)
	if err != nil {
		return 0, fmt.Errorf("create replication slot: %w", err)
	}

	return pglogrepl.ParseLSN(result.ConsistentPoint)
}

// slotIsExists checks whether a slot has already been created and if it has been created uses it.
func (l *Listener) slotIsExists(ctx context.Context) (bool, error) {
	restartLSNStr, err := l.repository.GetSlotLSN(ctx, l.cfg.Listener.SlotName)
	if err != nil {
		return false, fmt.Errorf("get slot lsn: %w", err)
	}

	if len(restartLSNStr) == 0 {
		l.log.Warn("restart LSN not found", slog.String("slot_name", l.cfg.Listener.SlotName))
		return false, nil
	}

	lsn, err := pglogrepl.ParseLSN(restartLSNStr)
	if err != nil {
		return false, fmt.Errorf("parse lsn: %w", err)
	}

	l.setLSN(ctx, lsn)

	return true, nil
}

const (
	protoVersion    = "proto_version '1'"
	publicationName = "pgoutbox"
)

const (
	problemKindParse   = "parse"
	problemKindPublish = "publish"
	problemKindAck     = "ack"
)

// Stream receives event from PostgreSQL.
// Accept message, apply filter and publish it in NATS server.
func (l *Listener) Stream(ctx context.Context) error {
	pluginArgs := []string{protoVersion, publicationNames(publicationName)}

	if err := l.replicator.StartReplication(
		ctx,
		l.cfg.Listener.SlotName,
		l.readLSN(),
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs},
	); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	go l.SendPeriodicHeartbeats(ctx)

	pool := &sync.Pool{
		New: func() any {
			return &apis.Event{}
		},
	}

	txWAL := tx.NewWAL(l.log, pool, l.monitor)

	for {
		if err := ctx.Err(); err != nil {
			l.log.Warn("stream: context canceled", "err", err)
			return nil
		}

		rawMsg, err := l.replicator.ReceiveMessage(ctx)
		if err != nil {
			return fmt.Errorf("receive message: %w", err)
		}

		if rawMsg == nil {
			l.log.Debug("got empty message")
			continue
		}

		start := time.Now()
		if err = l.processRawMessage(ctx, rawMsg, txWAL); err != nil {
			return fmt.Errorf("process message: %w", err)
		}
		l.monitor.RecordProcessingDuration(ctx, time.Since(start).Seconds())
	}
}

func (l *Listener) processRawMessage(ctx context.Context, rawMsg []byte, txWAL *tx.WAL) error {
	if len(rawMsg) == 0 {
		l.log.Debug("empty raw message")
		return nil
	}

	msgType := rawMsg[0]

	switch msgType {
	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(rawMsg[1:])
		if err != nil {
			return fmt.Errorf("parse xlog data: %w", err)
		}

		l.log.Debug("WAL message has been received", slog.String("wal", xld.WALStart.String()))

		if err := l.parser.ParseWalMessage(xld.WALData, txWAL); err != nil {
			l.monitor.IncProblematicEvents(ctx, problemKindParse)
			return fmt.Errorf("parse: %w", err)
		}

		if txWAL.CommitTime != nil {
			for event := range txWAL.CreateEventsWithFilter(ctx, l.cfg.Listener.Filter.Tables) {
				subjectName := event.SubjectName(l.cfg)

				publishStart := time.Now()
				if err := l.publisher.Publish(ctx, subjectName, event); err != nil {
					l.monitor.IncProblematicEvents(ctx, problemKindPublish)
					return fmt.Errorf("publish: %w", err)
				}
				l.monitor.RecordPublishDuration(ctx, time.Since(publishStart).Seconds(), subjectName)
				l.monitor.IncPublishedEvents(ctx, subjectName, event.Table)

				l.log.Info(
					"event was sent",
					slog.String("subject", subjectName),
					slog.String("action", event.Action),
					slog.String("table", event.Table),
					slog.String("lsn", l.readLSN().String()),
				)

				txWAL.RetrieveEvent(event)
			}

			txWAL.Clear()
		}

		if xld.WALStart > l.readLSN() {
			if err := l.AckWalMessage(ctx, xld.WALStart); err != nil {
				l.monitor.IncProblematicEvents(ctx, problemKindAck)
				return fmt.Errorf("ack: %w", err)
			}

			l.log.Debug("ack WAL message", slog.String("lsn", l.readLSN().String()))
		}

	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(rawMsg[1:])
		if err != nil {
			return fmt.Errorf("parse primary keepalive: %w", err)
		}

		l.log.Debug(
			"received server heartbeat",
			slog.String("server_wal_end", pkm.ServerWALEnd.String()),
			slog.Time("server_time", pkm.ServerTime),
		)

		if pkm.ServerWALEnd > l.readLSN() {
			l.setLSN(ctx, pkm.ServerWALEnd)
		}

		if pkm.ReplyRequested {
			l.log.Debug("status requested")

			if err := l.SendStandbyStatus(ctx); err != nil {
				l.log.Warn("send standby status", "err", err)
			}
		}
	}

	return nil
}

func publicationNames(publication string) string {
	return fmt.Sprintf(`publication_names '%s'`, publication)
}

// Stop is a finalizer function.
func (l *Listener) Stop() error {
	if err := l.repository.Close(context.Background()); err != nil {
		return fmt.Errorf("repository close: %w", err)
	}

	if err := l.replicator.Close(); err != nil {
		return fmt.Errorf("replicator close: %w", err)
	}

	l.log.Info("service was stopped")

	return nil
}

// SendPeriodicHeartbeats send periodic keep living heartbeats to the server.
func (l *Listener) SendPeriodicHeartbeats(ctx context.Context) {
	heart := time.NewTicker(l.cfg.Listener.HeartbeatInterval)
	defer heart.Stop()

	for {
		select {
		case <-ctx.Done():
			l.log.Warn("periodic heartbeats: context was canceled")
			return
		case <-heart.C:
			if err := l.SendStandbyStatus(ctx); err != nil {
				l.log.Error("failed to send heartbeat status", "err", err)
				l.isAlive.Store(false)

				continue
			}

			l.isAlive.Store(true)
			l.log.Debug("sending periodic heartbeat status")
		}
	}
}

// SendStandbyStatus sends a StandbyStatusUpdate with the current RestartLSN value to the server.
func (l *Listener) SendStandbyStatus(ctx context.Context) error {
	lsn := l.readLSN()

	status := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
	}

	if err := l.replicator.SendStandbyStatusUpdate(ctx, status); err != nil {
		return fmt.Errorf("unable to send StandbyStatusUpdate: %w", err)
	}

	return nil
}

// AckWalMessage acknowledge received wal message.
func (l *Listener) AckWalMessage(ctx context.Context, lsn pglogrepl.LSN) error {
	l.setLSN(ctx, lsn)

	if err := l.SendStandbyStatus(ctx); err != nil {
		return fmt.Errorf("send status: %w", err)
	}

	return nil
}

func (l *Listener) readLSN() pglogrepl.LSN {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.lsn
}

func (l *Listener) setLSN(ctx context.Context, lsn pglogrepl.LSN) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lsn = lsn
	l.monitor.RecordLSN(ctx, int64(l.lsn))
}
