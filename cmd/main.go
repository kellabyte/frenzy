package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/akamensky/argparse"
	"github.com/kellabyte/frenzy/server"
)

func main() {
	parser := argparse.NewParser("print", "Frenzy Mirroring Postgres Proxy")
	listenAddress := parser.String("l", "listen", &argparse.Options{Required: true, Help: "Listening port."})
	primaryAddress := parser.String("p", "primary", &argparse.Options{Required: true, Help: "Primary Postgres connection string."})
	var mirrorsAddresses *[]string = parser.StringList("m", "mirror", &argparse.Options{Required: true, Help: "Mirror Postgres connection string."})

	// Parse input
	err := parser.Parse(os.Args)
	if err != nil {
		// In case of error print error and print usage
		// This can also be done by passing -h or --help flags
		fmt.Print(parser.Usage(err))
		fmt.Printf("EXAMPLE\nfrenzy --listen :5432 --primary postgresql://postgres:password@localhost:5441/postgres --mirror postgresql://postgres:password@localhost:5442/postgres")
		os.Exit(1)
	}

	log.Printf("Frenzy starting up")
	log.Printf("Listen: %s", *listenAddress)
	log.Printf("Primary: %s", *primaryAddress)
	log.Printf("Mirrors: %v", strings.Join(*mirrorsAddresses, ", "))

	server := server.NewProxyServer()
	server.ListenAndServe(context.Background(), *listenAddress, *primaryAddress, *mirrorsAddresses)
	defer server.Close(context.Background())
}
