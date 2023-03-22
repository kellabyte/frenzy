package server

import (
	"context"
	"log"
	"strconv"

	wire "github.com/jeroenrinzema/psql-wire"
	"go.uber.org/zap"
)

type ProxyServer struct {
	logger  *zap.Logger
	primary *Connection
	mirrors []*Connection
}

func NewProxyServer(logger *zap.Logger) *ProxyServer {
	return &ProxyServer{
		logger: logger,
	}
}

func (server *ProxyServer) ListenAndServe(
	ctx context.Context,
	listenAddress string,
	primaryAddress string,
	mirrorAddresses []string) error {

	primaryName := "primary"
	primaryConnection := NewConnection(server.logger.Named(primaryName), Primary, primaryName)
	err := primaryConnection.Connect(ctx, primaryAddress)
	if err != nil {
		return err
	}
	server.primary = primaryConnection

	err = server.connectToMirrors(ctx, mirrorAddresses)
	if err != nil {
		return err
	}

	var postgresListener *wire.Server
	postgresListener, err = wire.NewServer(wire.SimpleQuery(server.handle), wire.Logger(server.logger))
	if err != nil {
		return err
	}
	server.adoptPostgresVersion(postgresListener)
	postgresListener.ListenAndServe(listenAddress)
	return nil
}

func (server *ProxyServer) adoptPostgresVersion(postgresListener *wire.Server) {
	// We adopt and announce as the same version as the Primary.
	postgresListener.Version = server.primary.pgServerVersion
}

func (server *ProxyServer) connectToMirrors(
	ctx context.Context,
	mirrorAddresses []string) error {

	for index, mirrorAddress := range mirrorAddresses {
		name := "mirror-" + strconv.Itoa(index+1)
		connection := NewConnection(server.logger.Named(name), Mirror, name)
		err := connection.Connect(ctx, mirrorAddress)
		if err != nil {
			return err
		}
		server.mirrors = append(server.mirrors, connection)
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

func (server *ProxyServer) handle(
	ctx context.Context,
	query string,
	writer wire.DataWriter,
	parameters []string) error {

	log.Println("QUERY:", query)

	err := server.primary.ExecuteQuery(ctx, query, writer)
	if err != nil {
		return err
	}
	for _, mirror := range server.mirrors {
		mirror.ExecuteQuery(ctx, query, writer)
	}
	return err
}
