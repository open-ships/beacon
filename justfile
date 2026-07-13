binary := "beacon"
cmd    := "./cmd/beacon"
image  := "beacon"
version := `git describe --tags --always --dirty 2>/dev/null || echo "dev"`

# list available recipes
default:
    @just --list

# build the binary locally
build:
    go build -ldflags "-X main.version={{version}}" -o {{binary}} {{cmd}}

# run all tests
test:
    go test $(go list ./... | grep -v '^github.com/open-ships/beacon/cmd/')

# run tests with verbose output
test-v:
    go test -v ./...

# run tests with race detector
test-race:
    go test -race ./...

# run the binary (pass args after --)
run *args:
    go run {{cmd}} {{args}}

# rebuild internal/ui/assets/app.css from internal/ui/uisrc/input.css
# (requires internal/ui/uisrc/build/ — see internal/ui/uisrc/README.md to fetch it)
ui-css:
    internal/ui/uisrc/build/tailwindcss-macos-arm64 \
        -i internal/ui/uisrc/input.css \
        -o internal/ui/assets/app.css \
        --minify

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
    GOOS=linux GOARCH=arm64 go build -ldflags "-X main.version={{version}}" -o {{binary}}-arm64 {{cmd}}

# cross-compile for Linux amd64
build-amd64:
    GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version={{version}}" -o {{binary}}-amd64 {{cmd}}

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
