package transaction

import "context"

type monitorMock struct{}

func (m *monitorMock) IncPublishedEvents(ctx context.Context, subject, table string) {}

func (m *monitorMock) IncFilterSkippedEvents(ctx context.Context, table string) {}

func (m *monitorMock) IncProblematicEvents(ctx context.Context, kind string) {}

func (m *monitorMock) RecordProcessingDuration(ctx context.Context, seconds float64) {}

func (m *monitorMock) RecordPublishDuration(ctx context.Context, seconds float64, subject string) {}

func (m *monitorMock) RecordLSN(ctx context.Context, lsn int64) {}
