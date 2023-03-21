all: deps build

deps:
	go get -d ./...

build:
	go build -o bin/frenzy ./cmd