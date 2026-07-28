package listener

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"kubeops.dev/pgoutbox/apis"
	tx "kubeops.dev/pgoutbox/internal/listener/transaction"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var errSimple = errors.New("some err")

func TestListener_slotIsExists(t *testing.T) {
	type fields struct {
		slotName string
	}

	repo := new(repositoryMock)
	metrics := new(monitorMock)

	setGetSlotLSN := func(slotName, lsn string, err error) {
		repo.On("GetSlotLSN", mock.Anything, slotName).
			Return(lsn, err).
			Once()
	}
	tests := []struct {
		name    string
		setup   func()
		fields  fields
		want    bool
		wantErr bool
	}{
		{
			name: "slot is exists",
			setup: func() {
				setGetSlotLSN("myslot", "0/17843B8", nil)
			},
			fields: fields{
				slotName: "myslot",
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "empty lsn",
			setup: func() {
				setGetSlotLSN("myslot", "", nil)
			},
			fields: fields{
				slotName: "myslot",
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "invalid lsn",
			setup: func() {
				setGetSlotLSN("myslot", "invalid", nil)
			},
			fields: fields{
				slotName: "myslot",
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "repository error",
			setup: func() {
				setGetSlotLSN("myslot", "", errSimple)
			},
			fields: fields{
				slotName: "myslot",
			},
			want:    false,
			wantErr: true,
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()

			w := &Listener{
				log: logger,
				cfg: &apis.Config{Listener: &apis.ListenerCfg{
					SlotName: tt.fields.slotName,
				}},
				monitor:    metrics,
				repository: repo,
			}

			got, err := w.slotIsExists(context.Background())
			if (err != nil) != tt.wantErr {
				t.Errorf("slotIsExists() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got != tt.want {
				t.Errorf("slotIsExists() got = %v, want %v", got, tt.want)
			}

			repo.AssertExpectations(t)
		})
	}
}

func TestListener_Stop(t *testing.T) {
	repo := new(repositoryMock)
	publ := new(publisherMock)
	repl := new(replicatorMock)

	setRepoClose := func(err error) {
		repo.On("Close", mock.Anything).
			Return(err).
			Once()
	}

	setReplClose := func(err error) {
		repl.On("Close").
			Return(err).
			Once()
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	tests := []struct {
		name    string
		setup   func()
		wantErr error
	}{
		{
			name: "success",
			setup: func() {
				setRepoClose(nil)
				setReplClose(nil)
			},
			wantErr: nil,
		},
		{
			name: "repository error",
			setup: func() {
				setRepoClose(errors.New("repo err"))
			},
			wantErr: errors.New("repository close: repo err"),
		},
		{
			name: "replication error",
			setup: func() {
				setReplClose(errors.New("replication err"))
				setRepoClose(nil)
			},
			wantErr: errors.New("replicator close: replication err"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			w := &Listener{
				log:        logger,
				publisher:  publ,
				replicator: repl,
				repository: repo,
			}
			err := w.Stop()
			if err != nil && assert.Error(t, tt.wantErr) {
				assert.EqualError(t, err, tt.wantErr.Error())
			}

			repo.AssertExpectations(t)
			repl.AssertExpectations(t)
			publ.AssertExpectations(t)
		})
	}
}

func TestListener_SendStandbyStatus(t *testing.T) {
	type fields struct {
		restartLSN pglogrepl.LSN
	}

	repl := new(replicatorMock)

	setSendStandbyStatusUpdate := func(lsn pglogrepl.LSN, err error) {
		repl.On(
			"SendStandbyStatusUpdate",
			mock.Anything,
			mock.MatchedBy(func(status pglogrepl.StandbyStatusUpdate) bool {
				return status.WALWritePosition == lsn
			}),
		).
			Return(err).
			Once()
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	tests := []struct {
		name    string
		setup   func()
		fields  fields
		wantErr bool
	}{
		{
			name: "success",
			setup: func() {
				setSendStandbyStatusUpdate(10, nil)
			},
			fields: fields{
				restartLSN: 10,
			},
			wantErr: false,
		},
		{
			name: "some replicator err",
			setup: func() {
				setSendStandbyStatusUpdate(10, errSimple)
			},
			fields: fields{
				restartLSN: 10,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer repl.AssertExpectations(t)

			tt.setup()

			w := &Listener{
				log:        logger,
				replicator: repl,
				lsn:        tt.fields.restartLSN,
			}

			if err := w.SendStandbyStatus(context.Background()); (err != nil) != tt.wantErr {
				t.Errorf("SendStandbyStatus() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestListener_AckWalMessage(t *testing.T) {
	type fields struct {
		restartLSN pglogrepl.LSN
	}

	type args struct {
		LSN pglogrepl.LSN
	}

	repl := new(replicatorMock)
	metrics := new(monitorMock)

	setSendStandbyStatusUpdate := func(lsn pglogrepl.LSN, err error) {
		repl.On(
			"SendStandbyStatusUpdate",
			mock.Anything,
			mock.MatchedBy(func(status pglogrepl.StandbyStatusUpdate) bool {
				return status.WALWritePosition == lsn
			}),
		).
			Return(err).
			Once()
	}

	tests := []struct {
		name    string
		setup   func()
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "success",
			setup: func() {
				setSendStandbyStatusUpdate(24658872, nil)
			},
			fields: fields{
				restartLSN: 0,
			},
			args: args{
				LSN: 24658872,
			},
			wantErr: false,
		},
		{
			name: "send status error",
			setup: func() {
				setSendStandbyStatusUpdate(24658872, errSimple)
			},
			fields: fields{
				restartLSN: 0,
			},
			args: args{
				LSN: 24658872,
			},
			wantErr: true,
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			w := &Listener{
				log:        logger,
				replicator: repl,
				lsn:        tt.fields.restartLSN,
				monitor:    metrics,
			}
			if err := w.AckWalMessage(context.Background(), tt.args.LSN); (err != nil) != tt.wantErr {
				t.Errorf("AckWalMessage() error = %v, wantErr %v", err, tt.wantErr)
			}

			repl.AssertExpectations(t)
		})
	}
}

func TestListener_Stream(t *testing.T) {
	t.Skip() // FIXME: Needs full rewrite for pglogrepl API

	repo := new(repositoryMock)
	publ := new(publisherMock)
	repl := new(replicatorMock)
	prs := new(parserMock)

	type fields struct {
		config     *apis.Config
		slotName   string
		restartLSN pglogrepl.LSN
	}

	type args struct {
		timeout time.Duration
	}

	setParseWalMessageOnce := func(msg []byte, tx *tx.WAL, err error) {
		prs.On("ParseWalMessage", msg, tx).Return(err)
	}

	setStartReplication := func(err error, slotName string, startLsn pglogrepl.LSN, options pglogrepl.StartReplicationOptions) {
		repl.On(
			"StartReplication",
			mock.Anything,
			slotName,
			startLsn,
			options,
		).Return(err)
	}

	setReceiveMessage := func(msg []byte, err error) {
		repl.On(
			"ReceiveMessage",
			mock.Anything,
		).Return(msg, err).After(10 * time.Millisecond)
	}

	setSendStandbyStatusUpdate := func(lsn pglogrepl.LSN, err error) {
		repl.On(
			"SendStandbyStatusUpdate",
			mock.Anything,
			mock.MatchedBy(func(status pglogrepl.StandbyStatusUpdate) bool {
				return status.WALWritePosition == lsn
			}),
		).Return(err).After(10 * time.Millisecond)
	}

	setPublish := func(subject string, want apis.Event, err error) {
		publ.On("Publish", mock.Anything, subject, mock.MatchedBy(func(got *apis.Event) bool {
			ok := want.Action == got.Action &&
				reflect.DeepEqual(want.Data, got.Data) &&
				want.ID == got.ID &&
				want.Schema == got.Schema &&
				want.Table == got.Table &&
				want.EventTime.Sub(got.EventTime).Milliseconds() < 1000
			if !ok {
				t.Errorf("- want + got\n- %#+v\n+ %#+v", want, got)
			}
			return ok
		})).Return(err)
	}

	uuid.SetRand(bytes.NewReader(make([]byte, 512)))

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	metrics := new(monitorMock)

	tests := []struct {
		name    string
		setup   func()
		fields  fields
		args    args
		wantErr error
	}{
		{
			name: "success",
			setup: func() {
				setStartReplication(
					nil,
					"myslot",
					pglogrepl.LSN(0),
					pglogrepl.StartReplicationOptions{PluginArgs: []string{protoVersion, "publication_names 'pgoutbox'"}},
				)

				setSendStandbyStatusUpdate(10, nil)

				setParseWalMessageOnce(
					[]byte(`some bytes`),
					tx.NewWAL(logger, nil, metrics),
					nil,
				)

				setPublish(
					"STREAM.pre_public_users",
					apis.Event{
						ID:        uuid.MustParse("00000000-0000-4000-8000-000000000000"),
						Schema:    "public",
						Table:     "users",
						Action:    "INSERT",
						Data:      map[string]any{"id": 1},
						EventTime: time.Now(),
					},
					nil,
				)

				// XLogData message with byte ID prefix
				setReceiveMessage(nil, nil)
			},
			fields: fields{
				config: &apis.Config{
					Listener: &apis.ListenerCfg{
						SlotName:          "myslot",
						AckTimeout:        0,
						HeartbeatInterval: 5 * time.Millisecond,
						Filter: apis.FilterStruct{
							Tables: map[string][]string{"users": {"insert"}},
						},
					},
					Publisher: &apis.PublisherCfg{
						Topic:       "STREAM",
						TopicPrefix: "pre_",
					},
				},
				slotName:   "myslot",
				restartLSN: 0,
			},
			args: args{
				timeout: 5 * time.Millisecond,
			},
		},
		{
			name: "start replication err",
			setup: func() {
				setStartReplication(
					errSimple,
					"myslot",
					pglogrepl.LSN(0),
					pglogrepl.StartReplicationOptions{PluginArgs: []string{protoVersion, "publication_names 'pgoutbox'"}},
				)
			},
			fields: fields{
				config: &apis.Config{
					Listener: &apis.ListenerCfg{
						SlotName:          "myslot",
						AckTimeout:        0,
						HeartbeatInterval: 1, Filter: apis.FilterStruct{
							Tables: map[string][]string{"users": {"insert"}},
						},
					},
					Publisher: &apis.PublisherCfg{
						Topic:       "stream",
						TopicPrefix: "pre_",
					},
				},
				slotName:   "myslot",
				restartLSN: 0,
			},
			args: args{
				timeout: 100 * time.Microsecond,
			},
			wantErr: errors.New("start replication: some err"),
		},
		{
			name: "receive message err",
			setup: func() {
				setStartReplication(
					nil,
					"myslot",
					pglogrepl.LSN(0),
					pglogrepl.StartReplicationOptions{PluginArgs: []string{protoVersion, "publication_names 'pgoutbox'"}},
				)

				setSendStandbyStatusUpdate(10, nil)

				setReceiveMessage(nil, errSimple)
			},
			fields: fields{
				config: &apis.Config{
					Listener: &apis.ListenerCfg{
						SlotName:          "myslot",
						AckTimeout:        0,
						HeartbeatInterval: 1, Filter: apis.FilterStruct{
							Tables: map[string][]string{"users": {"insert"}},
						},
					},
					Publisher: &apis.PublisherCfg{
						Topic:       "stream",
						TopicPrefix: "pre_",
					},
				},
				slotName:   "myslot",
				restartLSN: 0,
			},
			args: args{
				timeout: 100 * time.Microsecond,
			},
			wantErr: errors.New("receive message: some err"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer repl.AssertExpectations(t)

			tt.setup()

			ctx, cancel := context.WithTimeout(context.Background(), tt.args.timeout)
			_ = cancel

			w := &Listener{
				log:        logger,
				monitor:    metrics,
				cfg:        tt.fields.config,
				publisher:  publ,
				replicator: repl,
				repository: repo,
				parser:     prs,
				lsn:        tt.fields.restartLSN,
			}

			if err := w.Stream(ctx); err != nil && assert.Error(t, tt.wantErr, err.Error()) {
				assert.EqualError(t, err, tt.wantErr.Error())
			} else {
				assert.NoError(t, err)
			}

			repl.ExpectedCalls = nil
		})
	}
}

func TestListener_StreamShutdown(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	cfg := &apis.Config{Listener: &apis.ListenerCfg{
		SlotName:          "myslot",
		HeartbeatInterval: time.Millisecond,
	}}

	// blockUntilDone stands in for a read parked in ReceiveMessage. pgconn aborts
	// it by setting a deadline on the socket, so the error is an I/O timeout and
	// carries nothing about the context.
	blockUntilDone := func(args mock.Arguments) {
		<-args.Get(0).(context.Context).Done()
	}

	newStream := func(repl *replicatorMock) *Listener {
		repl.On("StartReplication", mock.Anything, "myslot", mock.Anything, mock.Anything).Return(nil)
		repl.On("SendStandbyStatusUpdate", mock.Anything, mock.Anything).Return(nil)

		return NewWalListener(cfg, logger, new(repositoryMock), repl, new(publisherMock),
			new(parserMock), new(monitorMock))
	}

	t.Run("a canceled context is a clean stop, not a failure", func(t *testing.T) {
		repl := new(replicatorMock)
		repl.On("ReceiveMessage", mock.Anything).
			Run(blockUntilDone).
			Return(nil, errors.New("i/o timeout"))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// Nil, otherwise every SIGTERM would exit non-zero and a rolling update
		// would look like a crash.
		assert.NoError(t, newStream(repl).Stream(ctx))
	})

	// A receive failure returns from Stream while the context is still live, which
	// is the case where the heartbeat has nothing to stop it on its own.
	t.Run("the heartbeat does not outlive a failed Stream", func(t *testing.T) {
		repl := new(replicatorMock)
		repl.On("ReceiveMessage", mock.Anything).
			Run(func(mock.Arguments) { time.Sleep(5 * time.Millisecond) }).
			Return(nil, errSimple)

		assert.ErrorIs(t, newStream(repl).Stream(context.Background()), errSimple)

		// Stream returning has to mean the heartbeat is gone: the caller closes the
		// replication connection next, and the heartbeat writes to it.
		sent := len(repl.Calls)
		time.Sleep(20 * time.Millisecond)
		assert.Equal(t, sent, len(repl.Calls), "heartbeat kept sending after Stream returned")
	})
}

func TestListener_checkConnection(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	cfg := &apis.Config{Listener: &apis.ListenerCfg{RefreshConnection: time.Millisecond}}

	newListener := func(repo *repositoryMock, repl *replicatorMock) *Listener {
		repl.On("IsAlive").Return(true)
		repo.On("IsAlive").Return(true)

		return NewWalListener(cfg, logger, repo, repl, new(publisherMock), new(parserMock), new(monitorMock))
	}

	t.Run("node was demoted by a failover", func(t *testing.T) {
		repo, repl := new(repositoryMock), new(replicatorMock)
		defer repo.AssertExpectations(t)

		repo.On("IsInRecovery", mock.Anything).Return(true, nil).Once()

		err := newListener(repo, repl).checkConnection(context.Background())
		assert.ErrorIs(t, err, errPrimaryDemoted)
	})

	t.Run("recovery check keeps failing", func(t *testing.T) {
		repo, repl := new(repositoryMock), new(replicatorMock)
		defer repo.AssertExpectations(t)

		repo.On("IsInRecovery", mock.Anything).
			Return(false, errors.New("timeout")).
			Times(maxRecoveryCheckFailures)

		err := newListener(repo, repl).checkConnection(context.Background())
		assert.ErrorIs(t, err, errConnectionIsLost)
	})

	t.Run("a single failed recovery check is tolerated", func(t *testing.T) {
		repo, repl := new(repositoryMock), new(replicatorMock)
		defer repo.AssertExpectations(t)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		repo.On("IsInRecovery", mock.Anything).Return(false, errors.New("timeout")).Once()
		// The failure counter resets, so the loop runs on until the context ends.
		repo.On("IsInRecovery", mock.Anything).
			Return(false, nil).
			Run(func(mock.Arguments) { cancel() })

		// Closing the connections is Process's job, once both goroutines have
		// returned, so cancelling here must leave them alone.
		assert.NoError(t, newListener(repo, repl).checkConnection(ctx))
		repo.AssertNotCalled(t, "Close", mock.Anything)
		repl.AssertNotCalled(t, "Close")
	})
}

func TestListener_Process(t *testing.T) {
	ctx := context.Background()
	monitor := new(monitorMock)
	parser := new(parserMock)
	repo := new(repositoryMock)
	repl := new(replicatorMock)
	pub := new(publisherMock)

	setCreatePublication := func(name string, err error) {
		repo.On("CreatePublication", mock.Anything, name).Return(err).Once()
	}

	setGetSlotLSN := func(slotName string, lsn string, err error) {
		repo.On("GetSlotLSN", mock.Anything, slotName).Return(lsn, err).Once()
	}

	setStartReplication := func(
		err error,
		slotName string,
		startLsn pglogrepl.LSN,
		options pglogrepl.StartReplicationOptions,
	) {
		repl.On("StartReplication", mock.Anything, slotName, startLsn, options).Return(err).Once()
	}

	setIsAlive := func(res bool) {
		repl.On("IsAlive").Return(res)
	}

	setClose := func(err error) {
		repl.On("Close").Return(err).Maybe()
	}

	setRepoClose := func(err error) {
		repo.On("Close", mock.Anything).Return(err)
	}

	setRepoIsAlive := func(res bool) {
		repo.On("IsAlive").Return(res)
	}

	setRepoIsInRecovery := func(res bool, err error) {
		repo.On("IsInRecovery", mock.Anything).Return(res, err)
	}

	setReceiveMessage := func(msg []byte, err error) {
		repl.On("ReceiveMessage", mock.Anything).Return(msg, err)
	}

	setSendStandbyStatusUpdate := func(err error) {
		repl.On("SendStandbyStatusUpdate", mock.Anything, mock.Anything).Return(err)
	}

	setCreateReplicationSlot := func(slotName, outputPlugin string, result pglogrepl.CreateReplicationSlotResult, err error) {
		repl.On("CreateReplicationSlot", mock.Anything, slotName, outputPlugin).Return(result, err)
	}

	setCreateFailoverSlot := func(slotName, lsn string, err error) {
		repo.On("CreateFailoverSlot", mock.Anything, slotName).Return(lsn, err)
	}

	setIsReplicationActive := func(slot string, res bool, err error) {
		repo.On("IsReplicationActive", mock.Anything, slot).Return(res, err)
	}

	tests := []struct {
		name    string
		cfg     *apis.Config
		setup   func()
		wantErr error
	}{
		{
			name: "success",
			cfg: &apis.Config{
				Listener: &apis.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: 1,
					HeartbeatInterval: 2,
					Filter: apis.FilterStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			setup: func() {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Millisecond*200)
				_ = cancel

				setIsReplicationActive("slot1", false, nil)

				setCreatePublication("pgoutbox", nil)
				setGetSlotLSN("slot1", "100/200", nil)
				setStartReplication(
					nil,
					"slot1",
					pglogrepl.LSN(1099511628288),
					pglogrepl.StartReplicationOptions{PluginArgs: []string{"proto_version '1'", "publication_names 'pgoutbox'"}},
				)
				setIsAlive(true)
				setRepoIsAlive(true)
				setRepoIsInRecovery(false, nil)
				setReceiveMessage(nil, nil)
				setSendStandbyStatusUpdate(nil)
				setClose(nil)
				setRepoClose(nil)
			},
			wantErr: nil,
		},
		{
			name: "skip create publication",
			cfg: &apis.Config{
				Listener: &apis.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: 1,
					HeartbeatInterval: 2,
					Filter: apis.FilterStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			setup: func() {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Millisecond*20)
				_ = cancel

				setCreatePublication("pgoutbox", errors.New("some err"))
				setGetSlotLSN("slot1", "100/200", nil)
				setStartReplication(
					nil,
					"slot1",
					pglogrepl.LSN(1099511628288),
					pglogrepl.StartReplicationOptions{PluginArgs: []string{"proto_version '1'", "publication_names 'pgoutbox'"}},
				)
				setIsAlive(true)
				setRepoIsAlive(true)
				setRepoIsInRecovery(false, nil)
				setReceiveMessage(nil, nil)
				setSendStandbyStatusUpdate(nil)
				setClose(nil)
				setRepoClose(nil)
			},
			wantErr: nil,
		},
		{
			name: "get slot error",
			cfg: &apis.Config{
				Listener: &apis.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: 1,
					HeartbeatInterval: 2,
					Filter: apis.FilterStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			setup: func() {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Millisecond*20)
				_ = cancel

				setCreatePublication("pgoutbox", nil)
				setGetSlotLSN("slot1", "100/200", errors.New("some err"))
			},
			wantErr: errors.New("slot is exists: get slot lsn: some err"),
		},
		{
			name: "slot does not exists",
			cfg: &apis.Config{
				Listener: &apis.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: 1,
					HeartbeatInterval: 2,
					Filter: apis.FilterStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			setup: func() {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Millisecond*20)
				_ = cancel

				setCreatePublication("pgoutbox", nil)
				setGetSlotLSN("slot1", "", nil)
				setCreateReplicationSlot(
					"slot1",
					"pgoutput",
					pglogrepl.CreateReplicationSlotResult{ConsistentPoint: "100/200"},
					nil,
				)
				setStartReplication(
					nil,
					"slot1",
					pglogrepl.LSN(1099511628288),
					pglogrepl.StartReplicationOptions{PluginArgs: []string{"proto_version '1'", "publication_names 'pgoutbox'"}},
				)
				setIsAlive(true)
				setRepoIsAlive(true)
				setRepoIsInRecovery(false, nil)
				setReceiveMessage(nil, nil)
				setSendStandbyStatusUpdate(nil)
				setClose(nil)
				setRepoClose(nil)
			},
			wantErr: nil,
		},
		{
			name: "slot does not exists with failover",
			cfg: &apis.Config{
				Listener: &apis.ListenerCfg{
					SlotName:          "slot1",
					Failover:          true,
					AckTimeout:        0,
					RefreshConnection: 1,
					HeartbeatInterval: 2,
					Filter: apis.FilterStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			setup: func() {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Millisecond*20)
				_ = cancel

				setCreatePublication("pgoutbox", nil)
				setGetSlotLSN("slot1", "", nil)
				// failover slots are created via the SQL function on the
				// repository connection, not the replication protocol.
				setCreateFailoverSlot("slot1", "100/200", nil)
				setStartReplication(
					nil,
					"slot1",
					pglogrepl.LSN(1099511628288),
					pglogrepl.StartReplicationOptions{PluginArgs: []string{"proto_version '1'", "publication_names 'pgoutbox'"}},
				)
				setIsAlive(true)
				setRepoIsAlive(true)
				setRepoIsInRecovery(false, nil)
				setReceiveMessage(nil, nil)
				setSendStandbyStatusUpdate(nil)
				setClose(nil)
				setRepoClose(nil)
			},
			wantErr: nil,
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer repo.AssertExpectations(t)
			defer repl.AssertExpectations(t)

			tt.setup()

			l := NewWalListener(
				tt.cfg,
				logger,
				repo,
				repl,
				pub,
				parser,
				monitor,
			)

			err := l.Process(ctx)
			if err != nil && assert.Error(t, tt.wantErr, err.Error()) {
				assert.EqualError(t, err, tt.wantErr.Error())
			} else {
				assert.NoError(t, tt.wantErr)
			}
		})
	}
}
