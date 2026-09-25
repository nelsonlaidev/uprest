compose := "docker compose -f examples/docker-compose.yml"

default:
    @just --list

build:
    go build -o bin/uprest ./cmd/uprest

run:
    go run ./cmd/uprest

clean:
    rm -rf bin

fmt:
    gofmt -l -w .
    golangci-lint fmt

lint:
    go vet ./...
    golangci-lint run

tidy:
    go mod tidy

test:
    go test ./...

test-race:
    go test -race ./...

test-integration redis="redis://localhost:6379":
    UPREST_TEST_REDIS_URL={{redis}} go test -race ./...

sdk-install:
    npm ci --prefix tests/sdk

sdk-test url="http://localhost:8079" token="example-token":
    UPREST_TEST_URL={{url}} UPREST_TEST_TOKEN={{token}} npm test --prefix tests/sdk

compatibility checkout url="http://localhost:8079" token="example-token":
    UPSTASH_REDIS_REST_URL={{url}} UPSTASH_REDIS_REST_TOKEN={{token}} tests/compatibility/run.sh {{checkout}}

changelog version:
    git-cliff --tag {{version}} --output CHANGELOG.md

up:
    {{compose}} up --build -d

down:
    {{compose}} down
