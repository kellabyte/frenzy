package server

import (
	"context"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	"go.uber.org/zap"
)

type ConnectionType int

const (
	Primary ConnectionType = iota
	Mirror
)

type Connection struct {
	logger          *zap.Logger
	name            string
	host            string
	conn            *pgx.Conn
	pgServerVersion string
	connectionType  ConnectionType
}

func NewConnection(logger *zap.Logger, connectionType ConnectionType, name string) *Connection {
	return &Connection{
		logger:         logger,
		connectionType: connectionType,
		name:           name,
	}
}

func (connection *Connection) Connect(
	ctx context.Context,
	hostAddress string) error {

	connection.host = hostAddress
	conn, err := pgx.Connect(ctx, connection.host)
	if err != nil {
		connection.logger.Error(
			"failed to connect to postgres",
			zap.String("address", connection.host),
			zap.Error(err))

		os.Exit(1)
	}
	connection.conn = conn
	connection.pgServerVersion, err = connection.getPostgresVersion(ctx)
	if err != nil {
		connection.logger.Error(
			"could not detect Postgres server_version_num",
			zap.Error(err))
	}

	if connection.connectionType == Primary {
		connection.logger.Info(
			"connected",
			zap.String("address", connection.host))

		if strings.TrimSpace(connection.pgServerVersion) == "" {
			connection.pgServerVersion = "150002"

			connection.logger.Error(
				"Could not detect primary server_version_num using default instead",
				zap.String("server_version_num", connection.pgServerVersion),
				zap.Error(err))
		}
		connection.logger.Info(
			"Detected and adopting primary server_version_num",
			zap.String("server_version_num", connection.pgServerVersion))

	} else if connection.connectionType == Mirror {
		connection.logger.Info(
			"Connected to mirror",
			zap.String("address", connection.host))
	}
	return nil
}

func (connection *Connection) getPostgresVersion(ctx context.Context) (string, error) {
	rows := connection.conn.QueryRow(ctx, "SELECT current_setting('server_version_num');")

	var pgServerVersion string
	err := rows.Scan(&pgServerVersion)
	if err != nil {
		return "", err
	}
	return pgServerVersion, nil
}

func (connection *Connection) Close(ctx context.Context) error {
	return connection.conn.Close(ctx)
}

func (connection *Connection) ExecuteQuery(
	ctx context.Context,
	query string,
	writer wire.DataWriter) error {

	if connection.connectionType == Primary {
		return connection.executePrimaryQuery(ctx, query, writer)
	} else if connection.connectionType == Mirror {
		return connection.executeMirrorQuery(ctx, query)
	}
	return nil
}

func (connection *Connection) executePrimaryQuery(
	ctx context.Context,
	query string,
	writer wire.DataWriter) error {

	rows, err := connection.conn.Query(ctx, query)
	if err != nil {
		connection.logger.Error(
			"Could not execute query on primary",
			zap.String("address", connection.host),
			zap.String("query", query),
			zap.Error(err))
		return err
	}

	table := wire.Columns{}

	// Add the columns.
	for _, field := range rows.FieldDescriptions() {
		dataTypeOID := oid.Oid(field.DataTypeOID)
		dataTypeName := oid.TypeName[dataTypeOID]
		connection.logger.Debug(
			"column read",
			zap.String("column", field.Name),
			zap.String("type", dataTypeName))

		// No idea what to do here. I suspect this is where I need to add support
		// for proxying various data types correctly and not treat everything as Text.
		column := wire.Column{
			Table: 0,
			Name:  field.Name,
			Oid:   dataTypeOID,
			// Width:
			Format: wire.TextFormat,
		}
		table = append(table, column)
	}
	err = writer.Define(table)
	if err != nil {
		return err
	}

	// Loop each row.
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return err
		}
		writer.Row(values)
	}
	err = writer.Complete("OK")
	if err != nil {
		return err
	}
	return nil
}

func (connection *Connection) executeMirrorQuery(
	ctx context.Context,
	query string) error {

	rows, err := connection.conn.Query(context.Background(), query)
	if err != nil {
		connection.logger.Error(
			"Could not execute query on mirror",
			zap.String("address", connection.host),
			zap.String("query", query),
			zap.Error(err))

		return err
	}
	defer rows.Close()
	return nil
}
