package server

import (
	"context"
	"log"
	"os"

	"github.com/jackc/pgx/v5"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
)

type InstanceType int

const (
	Primary InstanceType = iota
	Mirror
)

type ProxyServer struct {
	primary *pgx.Conn
	mirrors []*pgx.Conn
}

func NewProxyServer() *ProxyServer {
	return &ProxyServer{}
}

func (server *ProxyServer) ListenAndServe(ctx context.Context, listenAddress string, primaryAddress string, mirrorAddresses []string) error {
	err := server.connectToPostgres(ctx, primaryAddress, Primary)
	if err != nil {
		return err
	}
	err = server.connectToMirrors(ctx, mirrorAddresses)
	if err != nil {
		return err
	}

	log.Printf("PostgreSQL mirror is up and listening at [%s]", listenAddress)
	return wire.ListenAndServe(listenAddress, server.handle)
}

func (server *ProxyServer) connectToMirrors(ctx context.Context, mirrorAddresses []string) error {
	for _, mirrorAddress := range mirrorAddresses {
		err := server.connectToPostgres(ctx, mirrorAddress, Mirror)
		if err != nil {
			return err
		}
	}
	return nil
}

func (server *ProxyServer) connectToPostgres(ctx context.Context, hostAddress string, instanceType InstanceType) error {
	conn, err := pgx.Connect(ctx, hostAddress)
	if err != nil {
		log.Printf("Unable to connect to postgres [%s] %s", hostAddress, err)
		os.Exit(1)
	}
	if instanceType == Primary {
		log.Printf("Connected to primary [%s]", hostAddress)
		server.primary = conn
	} else if instanceType == Mirror {
		log.Printf("Connected to mirror [%s]", hostAddress)
		server.mirrors = append(server.mirrors, conn)
	}
	return nil
}

func (server *ProxyServer) Close(ctx context.Context) error {
	for _, mirror := range server.mirrors {
		err := mirror.Close(ctx)
		if err != nil {
			log.Printf("Failed to close mirror %s", err)
			return err
		}
	}
	return nil
}

func (server *ProxyServer) handle(ctx context.Context, query string, writer wire.DataWriter, parameters []string) error {
	log.Println("QUERY:", query)

	rows, err := server.handlePrimaryOperation(ctx, server.primary, query, writer)
	if err != nil {
		return err
	}
	for _, mirror := range server.mirrors {
		go server.mirrorOperation(ctx, mirror, query, parameters)
	}
	rows.Close()
	return err
}

func (server *ProxyServer) handlePrimaryOperation(ctx context.Context, conn *pgx.Conn, query string, writer wire.DataWriter) (pgx.Rows, error) {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		log.Printf("%x", err)
		return nil, err
	}

	table := wire.Columns{}

	// Add the columns.
	for _, field := range rows.FieldDescriptions() {
		dataTypeOID := oid.Oid(field.DataTypeOID)
		dataTypeName := oid.TypeName[dataTypeOID]
		log.Printf("FIELD: NAME: %s TYPE: %s", field.Name, dataTypeName)

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
		return nil, err
	}

	// Loop each row.
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		writer.Row(values)
	}
	err = writer.Complete("OK")
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (server *ProxyServer) mirrorOperation(ctx context.Context, conn *pgx.Conn, query string, parameters []string) (pgx.Rows, error) {
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		log.Printf("Unable to send query to mirror [%s] %s", query, err)
		return nil, err
	}
	defer rows.Close()
	return rows, nil
}

