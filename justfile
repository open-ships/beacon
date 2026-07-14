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

# run all tests
test:
    go test ./...

# run tests with verbose output
test-v:
    go test -v ./...

# run tests with the race detector, restricted to the packages that don't
# transitively import n2k/pgn: that package ICEs the Go compiler under -race
# (upstream bug, not beacon's — see internal/msg for the import boundary that
# contains it). `go list -deps -test` confirms this exact set; re-verify it
# whenever a package's imports change.
test-race:
    go test -race \
        ./internal/bus/busfake \
        ./internal/metrics \
        ./internal/model \
        ./internal/stats \
        ./internal/store \
        ./internal/sysinfo

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

# OpenBridge web-components release to vendor (see internal/ui/assets/README.md)
openbridge_version := "1.0.1"
# esbuild release used to minify the vendored bundle — pinned so the committed
# artifact is reproducible from the upstream download
esbuild_version := "0.28.1"

# re-vendor the OpenBridge assets: download the pinned release and minify the
# component bundle (upstream publishes no minified build; ~11.9MB -> ~2.9MB).
# Dev-time only — needs network + Node for npx; nothing here runs at build or
# runtime, the outputs are committed and embedded via go:embed. A test
# (TestOpenBridgeBundleIsMinified) fails if an unminified bundle is committed.
vendor-openbridge:
    curl -fsSL -o /tmp/openbridge.bundle.unmin.js \
        "https://cdn.jsdelivr.net/npm/@oicl/openbridge-webcomponents@{{openbridge_version}}/bundle/openbridge-webcomponents.bundle.js"
    npx -y esbuild@{{esbuild_version}} /tmp/openbridge.bundle.unmin.js \
        --minify --outfile=internal/ui/assets/openbridge.bundle.js
    curl -fsSL -o internal/ui/assets/palettes.css \
        "https://cdn.jsdelivr.net/npm/@oicl/openbridge-webcomponents@{{openbridge_version}}/src/palettes/variables.css"
    curl -fsSL -o internal/ui/assets/NotoSans.ttf \
        "https://cdn.jsdelivr.net/npm/@oicl/openbridge-webcomponents@{{openbridge_version}}/bundle/NotoSans.ttf"

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
