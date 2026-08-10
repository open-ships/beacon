package ui

import "testing"

func TestVersionMetadata(t *testing.T) {
	tests := []struct {
		name        string
		version     string
		wantLabel   string
		wantRelease string
	}{
		{name: "goreleaser semantic version", version: "1.3.0", wantLabel: "v1.3.0", wantRelease: "https://github.com/open-ships/beacon/releases/tag/v1.3.0"},
		{name: "docker semantic tag", version: "v1.3.0", wantLabel: "v1.3.0", wantRelease: "https://github.com/open-ships/beacon/releases/tag/v1.3.0"},
		{name: "development asset hash", version: "dev-0123456789ab", wantLabel: "dev"},
		{name: "non-release build", version: "test", wantLabel: "test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			label, release := versionMetadata(tt.version)
			if label != tt.wantLabel || release != tt.wantRelease {
				t.Fatalf("versionMetadata(%q) = (%q, %q), want (%q, %q)", tt.version, label, release, tt.wantLabel, tt.wantRelease)
			}
		})
	}
}
