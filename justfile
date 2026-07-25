binary := "beacon"
cmd    := "./cmd/beacon"
image  := "beacon"
version := `git describe --tags --always --dirty 2>/dev/null || echo "dev"`
golangci_lint_version := "v2.12.0"
secure_go_toolchain := "go1.25.12"
govulncheck_version := "v1.5.0"
gosec_version := "v2.27.1"
air_version := "v1.64.5"

# list available recipes
default:
    @just --list

# ensure dev tools are installed at the right versions
setup:
    @if command -v air >/dev/null && go version -m "$(command -v air)" 2>&1 | grep -q "github.com/air-verse/air[[:space:]]*{{air_version}}"; then \
        echo "air {{air_version}} already installed"; \
    else \
        echo "Installing air {{air_version}}..."; \
        go install github.com/air-verse/air@{{air_version}}; \
    fi
    @if command -v golangci-lint >/dev/null && golangci-lint --version 2>&1 | grep -q "{{trim_start_match(golangci_lint_version, "v")}}"; then \
        echo "golangci-lint {{golangci_lint_version}} already installed"; \
    else \
        echo "Installing golangci-lint {{golangci_lint_version}}..."; \
        curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b "$(go env GOPATH)/bin" {{golangci_lint_version}}; \
    fi
    @if command -v govulncheck >/dev/null && govulncheck -version 2>&1 | grep -q "govulncheck@{{govulncheck_version}}"; then \
        echo "govulncheck {{govulncheck_version}} already installed"; \
    else \
        echo "Installing govulncheck {{govulncheck_version}}..."; \
        go install golang.org/x/vuln/cmd/govulncheck@{{govulncheck_version}}; \
    fi
    @if command -v gosec >/dev/null && go version -m "$(command -v gosec)" 2>&1 | grep -q "github.com/securego/gosec/v2[[:space:]]*{{gosec_version}}"; then \
        echo "gosec {{gosec_version}} already installed"; \
    else \
        echo "Installing gosec {{gosec_version}}..."; \
        go install github.com/securego/gosec/v2/cmd/gosec@{{gosec_version}}; \
    fi

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

# run with automatic reloads when Go or embedded UI files change
dev *args:
    @if ! command -v air >/dev/null; then echo "air is required; run 'just setup' first"; exit 1; fi
    air -- {{args}}

# format all Go source files
fmt:
    gofmt -w .

# run go vet
vet:
    go vet ./...

# run golangci-lint
lint:
    golangci-lint run ./...

# run security review (vulnerability scan + static analysis)
secure:
    GOTOOLCHAIN={{secure_go_toolchain}} govulncheck ./...
    GOTOOLCHAIN={{secure_go_toolchain}} gosec -exclude-generated -exclude-dir=.claude ./...

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
