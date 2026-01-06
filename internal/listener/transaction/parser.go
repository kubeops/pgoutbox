package transaction

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pglogrepl"
)

// Parser represents a WAL message parser using pglogrepl.
type Parser struct {
	log *slog.Logger
}

var (
	ErrEmptyWALMessage    = errors.New("empty WAL message")
	ErrMessageLost        = errors.New("messages are lost")
	ErrUnknownMessageType = errors.New("unknown message type")
)

// NewParser creates a new instance of the WAL message parser.
func NewParser(logger *slog.Logger) *Parser {
	return &Parser{
		log: logger,
	}
}

// ParseWalMessage parses a postgres WAL message using pglogrepl.Parse().
func (p *Parser) ParseWalMessage(msg []byte, tx *WAL) error {
	if len(msg) == 0 {
		return ErrEmptyWALMessage
	}

	logicalMsg, err := pglogrepl.Parse(msg)
	if err != nil {
		return fmt.Errorf("pglogrepl parse: %w", err)
	}

	switch m := logicalMsg.(type) {
	case *pglogrepl.BeginMessage:
		p.log.Debug(
			"begin type message was received",
			slog.String("lsn", m.FinalLSN.String()),
			slog.Any("xid", m.Xid),
		)

		tx.LSN = m.FinalLSN
		tx.BeginTime = &m.CommitTime
	case *pglogrepl.CommitMessage:
		p.log.Debug(
			"commit message was received",
			slog.String("lsn", m.CommitLSN.String()),
			slog.String("transaction_lsn", m.TransactionEndLSN.String()),
		)

		if tx.LSN > 0 && tx.LSN != m.CommitLSN {
			return fmt.Errorf("commit: %w", ErrMessageLost)
		}

		tx.CommitTime = &m.CommitTime
	case *pglogrepl.OriginMessage:
		p.log.Debug("origin type message was received")
	case *pglogrepl.RelationMessage:
		p.log.Debug(
			"relation type message was received",
			slog.Any("relation_id", m.RelationID),
			slog.String("schema", m.Namespace),
		)

		if tx.LSN == 0 {
			return fmt.Errorf("relation: %w", ErrMessageLost)
		}

		rd := RelationData{
			Schema: m.Namespace,
			Table:  m.RelationName,
		}

		for _, col := range m.Columns {
			// Flags == 1 indicates the column is part of the key
			isKey := col.Flags == 1
			c := InitColumn(p.log, col.Name, nil, int(col.DataType), isKey)
			rd.Columns = append(rd.Columns, c)
		}

		tx.RelationStore[m.RelationID] = rd
	case *pglogrepl.TypeMessage:
		p.log.Debug("type message was received")
	case *pglogrepl.InsertMessage:
		p.log.Debug(
			"insert type message was received",
			slog.Any("relation_id", m.RelationID),
		)

		action, err := tx.CreateActionData(
			m.RelationID,
			nil,
			m.Tuple,
			ActionKindInsert,
		)
		if err != nil {
			return fmt.Errorf("create action data: %w", err)
		}

		tx.Actions = append(tx.Actions, action)
	case *pglogrepl.UpdateMessage:
		p.log.Debug("update type message was received", slog.Any("relation_id", m.RelationID))

		action, err := tx.CreateActionData(
			m.RelationID,
			m.OldTuple,
			m.NewTuple,
			ActionKindUpdate,
		)
		if err != nil {
			return fmt.Errorf("create action data: %w", err)
		}

		tx.Actions = append(tx.Actions, action)
	case *pglogrepl.DeleteMessage:
		p.log.Debug(
			"delete type message was received",
			slog.Any("relation_id", m.RelationID),
		)

		action, err := tx.CreateActionData(
			m.RelationID,
			m.OldTuple,
			nil,
			ActionKindDelete,
		)
		if err != nil {
			return fmt.Errorf("create action data: %w", err)
		}

		tx.Actions = append(tx.Actions, action)
	case *pglogrepl.TruncateMessage:
		p.log.Debug("truncate type message was received")
	case *pglogrepl.LogicalDecodingMessage:
		p.log.Debug("logical decoding message was received", slog.String("prefix", m.Prefix))
	default:
		return fmt.Errorf("%w: %T", ErrUnknownMessageType, logicalMsg)
	}

	return nil
}
