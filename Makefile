.PHONY: run test cover bench load build vet fmt docker

run:
	go run ./cmd/pulsed -addr=:8080

build:
	go build -o bin/pulsed ./cmd/pulsed
	go build -o bin/loadgen ./cmd/loadgen

test:
	go test -race -count=1 ./...

cover:
	go test -coverprofile=coverage.out -count=1 ./internal/...
	go tool cover -func=coverage.out | tail -1

bench:
	go test -run=XXX -bench=. -benchmem ./internal/telemetry/

vet:
	go vet ./...

fmt:
	gofmt -l -w .

load:
	go run ./cmd/loadgen -devices=500 -hz=2 -duration=30s

docker:
	docker build -t pulse:latest .
