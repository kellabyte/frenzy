package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/akamensky/argparse"
	"github.com/kellabyte/frenzy/server"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	logger, err := configureLogger()
	if err != nil {
		log.Fatal(err)
	}
	defer logger.Sync()

	parser := argparse.NewParser("print", "Frenzy Mirroring Postgres Proxy")
	listenAddress := parser.String("l", "listen", &argparse.Options{Required: true, Help: "Listening port."})
	primaryAddress := parser.String("p", "primary", &argparse.Options{Required: true, Help: "Primary Postgres connection string."})
	var mirrorsAddresses *[]string = parser.StringList("m", "mirror", &argparse.Options{Required: true, Help: "Mirror Postgres connection string."})

	// Parse input
	err = parser.Parse(os.Args)
	if err != nil {
		// In case of error print error and print usage
		// This can also be done by passing -h or --help flags
		fmt.Print(parser.Usage(err))
		fmt.Printf("EXAMPLE\nfrenzy --listen :5432 --primary postgresql://postgres:password@localhost:5441/postgres --mirror postgresql://postgres:password@localhost:5442/postgres")
		os.Exit(1)
	}

	mirrors := *mirrorsAddresses
	logFields := make([]zapcore.Field, 0)
	logFields = append(logFields, zap.String("listen", *listenAddress))
	logFields = append(logFields, zap.String("primary", *primaryAddress))
	for index, mirror := range mirrors {
		field := zapcore.Field{
			Key:    "mirror-" + strconv.Itoa((index + 1)),
			Type:   zapcore.StringType,
			String: mirror,
		}
		logFields = append(logFields, field)
	}

	logger.Info("frenzy starting up", logFields...)

	server := server.NewProxyServer(logger)
	server.ListenAndServe(context.Background(), *listenAddress, *primaryAddress, *mirrorsAddresses)
	defer server.Close(context.Background())
}

func configureLogger() (*zap.Logger, error) {
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, err := config.Build()
	return logger.Named("frenzy"), err
}
