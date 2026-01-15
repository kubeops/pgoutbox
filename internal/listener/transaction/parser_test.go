package transaction

import (
	"encoding/binary"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/assert"
)

var bigEndian = binary.BigEndian

// Helper to build BeginMessage bytes
func buildBeginMessage(finalLSN pglogrepl.LSN, commitTime time.Time, xid uint32) []byte {
	msg := make([]byte, 1+8+8+4)
	msg[0] = 'B'
	bigEndian.PutUint64(msg[1:], uint64(finalLSN))
	bigEndian.PutUint64(msg[9:], uint64(timeToPgTime(commitTime)))
	bigEndian.PutUint32(msg[17:], xid)
	return msg
}

// Helper to build CommitMessage bytes
func buildCommitMessage(flags uint8, commitLSN, transactionEndLSN pglogrepl.LSN, commitTime time.Time) []byte {
	msg := make([]byte, 1+1+8+8+8)
	msg[0] = 'C'
	msg[1] = flags
	bigEndian.PutUint64(msg[2:], uint64(commitLSN))
	bigEndian.PutUint64(msg[10:], uint64(transactionEndLSN))
	bigEndian.PutUint64(msg[18:], uint64(timeToPgTime(commitTime)))
	return msg
}

// Helper to build RelationMessage bytes
func buildRelationMessage(relationID uint32, namespace, relationName string, replicaIdentity uint8, columns []struct {
	flags    uint8
	name     string
	dataType uint32
	typeMod  int32
},
) []byte {
	// Calculate size
	size := 1 + 4 + len(namespace) + 1 + len(relationName) + 1 + 1 + 2
	for _, col := range columns {
		size += 1 + len(col.name) + 1 + 4 + 4
	}

	msg := make([]byte, size)
	off := 0
	msg[off] = 'R'
	off++
	bigEndian.PutUint32(msg[off:], relationID)
	off += 4
	copy(msg[off:], namespace)
	off += len(namespace)
	msg[off] = 0
	off++
	copy(msg[off:], relationName)
	off += len(relationName)
	msg[off] = 0
	off++
	msg[off] = replicaIdentity
	off++
	bigEndian.PutUint16(msg[off:], uint16(len(columns)))
	off += 2
	for _, col := range columns {
		msg[off] = col.flags
		off++
		copy(msg[off:], col.name)
		off += len(col.name)
		msg[off] = 0
		off++
		bigEndian.PutUint32(msg[off:], col.dataType)
		off += 4
		bigEndian.PutUint32(msg[off:], uint32(col.typeMod))
		off += 4
	}
	return msg
}

// Helper to build InsertMessage bytes
func buildInsertMessage(relationID uint32, tupleData [][]byte) []byte {
	tupleSize := 2
	for _, data := range tupleData {
		tupleSize += 1 + 4 + len(data) // type byte + length + data
	}

	msg := make([]byte, 1+4+1+tupleSize)
	off := 0
	msg[off] = 'I'
	off++
	bigEndian.PutUint32(msg[off:], relationID)
	off += 4
	msg[off] = 'N' // new tuple marker
	off++
	bigEndian.PutUint16(msg[off:], uint16(len(tupleData)))
	off += 2
	for _, data := range tupleData {
		msg[off] = 't' // text type
		off++
		bigEndian.PutUint32(msg[off:], uint32(len(data)))
		off += 4
		copy(msg[off:], data)
		off += len(data)
	}
	return msg
}

// timeToPgTime converts time.Time to PostgreSQL epoch microseconds
func timeToPgTime(t time.Time) int64 {
	pgEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	return t.Sub(pgEpoch).Microseconds()
}

func TestParser(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	tests := []struct {
		name      string
		setupWAL  func() *WAL
		buildMsg  func() []byte
		assertion func(t *testing.T, wal *WAL, err error)
	}{
		{
			name: "ParseWalMessage_BeginMessage",
			setupWAL: func() *WAL {
				return NewWAL(logger, nil, &monitorMock{})
			},
			buildMsg: func() []byte {
				finalLSN := pglogrepl.LSN(0x17843B8)
				commitTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
				xid := uint32(12345)
				return buildBeginMessage(finalLSN, commitTime, xid)
			},
			assertion: func(t *testing.T, wal *WAL, err error) {
				assert.NoError(t, err)
				assert.Equal(t, pglogrepl.LSN(0x17843B8), wal.LSN)
				assert.NotNil(t, wal.BeginTime)
				assert.WithinDuration(t, time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), *wal.BeginTime, time.Microsecond)
			},
		},
		{
			name: "ParseWalMessage_CommitMessage",
			setupWAL: func() *WAL {
				wal := NewWAL(logger, nil, &monitorMock{})
				wal.LSN = pglogrepl.LSN(0x17843B8)
				return wal
			},
			buildMsg: func() []byte {
				commitLSN := pglogrepl.LSN(0x17843B8)
				transactionEndLSN := pglogrepl.LSN(0x17843C0)
				commitTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
				return buildCommitMessage(0, commitLSN, transactionEndLSN, commitTime)
			},
			assertion: func(t *testing.T, wal *WAL, err error) {
				assert.NoError(t, err)
				assert.NotNil(t, wal.CommitTime)
				assert.WithinDuration(t, time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), *wal.CommitTime, time.Microsecond)
			},
		},
		{
			name: "ParseWalMessage_CommitMessage_LSNMismatch",
			setupWAL: func() *WAL {
				wal := NewWAL(logger, nil, &monitorMock{})
				wal.LSN = pglogrepl.LSN(0x17843C0)
				return wal
			},
			buildMsg: func() []byte {
				commitLSN := pglogrepl.LSN(0x17843B8)
				commitTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
				return buildCommitMessage(0, commitLSN, commitLSN, commitTime)
			},
			assertion: func(t *testing.T, wal *WAL, err error) {
				assert.Error(t, err)
				assert.ErrorIs(t, err, ErrMessageLost)
			},
		},
		{
			name: "ParseWalMessage_RelationMessage",
			setupWAL: func() *WAL {
				wal := NewWAL(logger, nil, &monitorMock{})
				wal.LSN = pglogrepl.LSN(0x17843B8)
				return wal
			},
			buildMsg: func() []byte {
				relationID := uint32(16385)
				namespace := "public"
				relationName := "users"
				columns := []struct {
					flags    uint8
					name     string
					dataType uint32
					typeMod  int32
				}{
					{flags: 1, name: "id", dataType: 23, typeMod: -1},      // int4, key
					{flags: 0, name: "name", dataType: 25, typeMod: -1},    // text
					{flags: 0, name: "email", dataType: 1043, typeMod: -1}, // varchar
				}
				return buildRelationMessage(relationID, namespace, relationName, 1, columns)
			},
			assertion: func(t *testing.T, wal *WAL, err error) {
				assert.NoError(t, err)
				relationID := uint32(16385)
				assert.Contains(t, wal.RelationStore, relationID)
				rd := wal.RelationStore[relationID]
				assert.Equal(t, "public", rd.Schema)
				assert.Equal(t, "users", rd.Table)
				assert.Len(t, rd.Columns, 3)
				assert.Equal(t, "id", rd.Columns[0].name)
				assert.True(t, rd.Columns[0].isKey)
				assert.Equal(t, "name", rd.Columns[1].name)
				assert.False(t, rd.Columns[1].isKey)
			},
		},
		{
			name: "ParseWalMessage_RelationMessage_NoLSN",
			setupWAL: func() *WAL {
				return NewWAL(logger, nil, &monitorMock{})
			},
			buildMsg: func() []byte {
				return buildRelationMessage(16385, "public", "users", 1, nil)
			},
			assertion: func(t *testing.T, wal *WAL, err error) {
				assert.Error(t, err)
				assert.ErrorIs(t, err, ErrMessageLost)
			},
		},
		{
			name: "ParseWalMessage_InsertMessage",
			setupWAL: func() *WAL {
				wal := NewWAL(logger, nil, &monitorMock{})
				wal.LSN = pglogrepl.LSN(0x17843B8)
				relationID := uint32(16385)
				wal.RelationStore[relationID] = RelationData{
					Schema: "public",
					Table:  "users",
					Columns: []Column{
						InitColumn(logger, "id", nil, Int4OID, true),
						InitColumn(logger, "name", nil, TextOID, false),
						InitColumn(logger, "email", nil, VarcharOID, false),
					},
				}
				return wal
			},
			buildMsg: func() []byte {
				relationID := uint32(16385)
				tupleData := [][]byte{
					[]byte("1"),
					[]byte("John Doe"),
					[]byte("john@example.com"),
				}
				return buildInsertMessage(relationID, tupleData)
			},
			assertion: func(t *testing.T, wal *WAL, err error) {
				assert.NoError(t, err)
				assert.Len(t, wal.Actions, 1)
				action := wal.Actions[0]
				assert.Equal(t, "public", action.Schema)
				assert.Equal(t, "users", action.Table)
				assert.Equal(t, ActionKindInsert, action.Kind)
				assert.Len(t, action.NewColumns, 3)
				assert.Equal(t, 1, action.NewColumns[0].value)
				assert.Equal(t, "John Doe", action.NewColumns[1].value)
				assert.Equal(t, "john@example.com", action.NewColumns[2].value)
			},
		},
		{
			name: "ParseWalMessage_EmptyMessage",
			setupWAL: func() *WAL {
				return NewWAL(logger, nil, &monitorMock{})
			},
			buildMsg: func() []byte {
				return []byte{}
			},
			assertion: func(t *testing.T, wal *WAL, err error) {
				assert.Error(t, err)
				assert.ErrorIs(t, err, ErrEmptyWALMessage)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(logger)
			wal := tt.setupWAL()
			msg := tt.buildMsg()
			err := parser.ParseWalMessage(msg, wal)
			tt.assertion(t, wal, err)
		})
	}
}
