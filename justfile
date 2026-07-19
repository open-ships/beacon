binary := "beacon"
cmd    := "./cmd/beacon"
image  := "beacon"
version := `git describe --tags --always --dirty 2>/dev/null || echo "dev"`

# list available recipes
default:
    @just --list

# build the binary locally
build:
    CGO_ENABLED=0 go build -ldflags "-X main.version={{version}}" -o {{binary}} {{cmd}}

# run all Go tests
test:
    go test ./...

# run tests with verbose output
test-v:
    go test -v ./...

# run browser end-to-end tests with Playwright
test-browser:
    npm test

# run browser end-to-end tests in Playwright's interactive UI
test-browser-ui:
    npm run test:ui

# run the full test suite with the race detector
test-race:
    go test -race ./...

# run the binary (pass args after --)
run *args:
    go run {{cmd}} {{args}}

# format all Go source files
fmt:
    gofmt -w .

# run go vet
vet:
    go vet ./...

# run golangci-lint
lint:
    golangci-lint run ./...

# remove build artifacts
clean:
    rm -f {{binary}}
    go clean ./...

# cross-compile for Linux arm64 (Raspberry Pi)
build-arm64:
    CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags "-X main.version={{version}}" -o {{binary}}-arm64 {{cmd}}

# cross-compile for Linux amd64
build-amd64:
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version={{version}}" -o {{binary}}-amd64 {{cmd}}

# build Docker image
docker-build:
    docker build --build-arg VERSION={{version}} -t {{image}}:{{version}} -t {{image}}:latest .

# run via docker compose
docker-run:
    docker compose up

# tidy go modules
tidy:
    go mod tidy

# show current version
version:
    @echo {{version}}
