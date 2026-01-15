package listener

import (
	"context"
	"time"

	"kubeops.dev/pgoutbox/apis"
	trx "kubeops.dev/pgoutbox/internal/listener/transaction"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/mock"
)

type monitorMock struct{}

func (m *monitorMock) IncPublishedEvents(ctx context.Context, subject, table string) {}

func (m *monitorMock) IncFilterSkippedEvents(ctx context.Context, table string) {}

func (m *monitorMock) IncProblematicEvents(ctx context.Context, kind string) {}

func (m *monitorMock) RecordProcessingDuration(ctx context.Context, seconds float64) {}

func (m *monitorMock) RecordPublishDuration(ctx context.Context, seconds float64, subject string) {}

func (m *monitorMock) RecordLSN(ctx context.Context, lsn int64) {}

type parserMock struct {
	mock.Mock
}

func (p *parserMock) ParseWalMessage(msg []byte, tx *trx.WAL) error {
	args := p.Called(msg, tx)
	now := time.Now()

	tx.BeginTime = &now
	tx.CommitTime = &now
	tx.Actions = []trx.ActionData{
		{
			Schema: "public",
			Table:  "users",
			Kind:   "INSERT",
			NewColumns: []trx.Column{
				trx.InitColumn(nil, "id", 1, 23, true),
			},
		},
	}

	return args.Error(0)
}

type publisherMock struct {
	mock.Mock
}

func (p *publisherMock) Publish(ctx context.Context, subject string, event *apis.Event) error {
	args := p.Called(ctx, subject, event)
	return args.Error(0)
}

type replicatorMock struct {
	mock.Mock
}

func (r *replicatorMock) CreateReplicationSlot(ctx context.Context, slotName, outputPlugin string) (pglogrepl.CreateReplicationSlotResult, error) {
	args := r.Called(ctx, slotName, outputPlugin)
	return args.Get(0).(pglogrepl.CreateReplicationSlotResult), args.Error(1)
}

func (r *replicatorMock) DropReplicationSlot(ctx context.Context, slotName string) error {
	args := r.Called(ctx, slotName)
	return args.Error(0)
}

func (r *replicatorMock) StartReplication(ctx context.Context, slotName string, startLsn pglogrepl.LSN, options pglogrepl.StartReplicationOptions) error {
	args := r.Called(ctx, slotName, startLsn, options)
	return args.Error(0)
}

func (r *replicatorMock) ReceiveMessage(ctx context.Context) ([]byte, error) {
	args := r.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (r *replicatorMock) SendStandbyStatusUpdate(ctx context.Context, status pglogrepl.StandbyStatusUpdate) error {
	return r.Called(ctx, status).Error(0)
}

func (r *replicatorMock) IsAlive() bool {
	return r.Called().Bool(0)
}

func (r *replicatorMock) Close() error {
	return r.Called().Error(0)
}
