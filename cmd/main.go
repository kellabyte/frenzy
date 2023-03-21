package main

import (
	"context"

	"github.com/kellabyte/frenzy/server"
)

func main() {
	// TODO: I haven't added CLI arguments yet.
	primaryAddress := "postgresql://postgres:password@localhost:5441/postgres"
	mirrorAddresses := []string{
		"postgresql://postgres:password@localhost:5442/postgres",
	}
	server := server.NewProxyServer()
	server.ListenAndServe(context.Background(), ":5432", primaryAddress, mirrorAddresses)
	defer server.Close(context.Background())
}
