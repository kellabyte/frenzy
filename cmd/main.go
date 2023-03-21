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
	parser := argparse.NewParser("print", "Frenzy Mirroring Proxy for Postgres")
	primary := parser.String("p", "primary", &argparse.Options{Required: true, Help: "Primary Postgres connection string"})
	var mirrors *[]string = parser.StringList("m", "mirror", &argparse.Options{Required: true, Help: "Mirror Postgres connection string"})

	// Parse input
	err := parser.Parse(os.Args)
	if err != nil {
		// In case of error print error and print usage
		// This can also be done by passing -h or --help flags
		fmt.Print(parser.Usage(err))
		os.Exit(1)
	}

	log.Printf("Frenzy starting up")
	log.Printf("Primary: %s", *primary)
	log.Printf("Mirrors: %v", strings.Join(*mirrors, ", "))

	server := server.NewProxyServer()
	server.ListenAndServe(context.Background(), ":5432", *primary, *mirrors)
	defer server.Close(context.Background())
}
