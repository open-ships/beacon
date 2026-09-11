package app

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/bus"
)

func TestAdminBaselineOriginProtection(t *testing.T) {
	for _, path := range []string{"/api/v1/n2k/inventory/baseline", "/n2k/inventory/baseline"} {
		t.Run(path, func(t *testing.T) {
			a := startTestApp(t)
			base := "http://" + a.AdminAddr()
			client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
			for i, tc := range []struct {
				name, origin, site string
				allowed            bool
			}{
				{"foreign origin", "https://foreign.example", "", false},
				{"foreign form", "https://foreign.example", "cross-site", false},
				{"fetch metadata", "", "cross-site", false},
				{"same-site is not same-origin", "", "same-site", false},
				{"same origin", base, "same-origin", true},
				{"CLI without browser headers", "", "", true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					name := uint64(i + 1)
					if err := a.inv.Observe(context.Background(), []bus.DeviceInfo{{Endpoint: "socketcan:can0", Name: name, LastSeen: time.Now()}}); err != nil {
						t.Fatal(err)
					}
					req, err := http.NewRequest(http.MethodPost, base+path, strings.NewReader(""))
					if err != nil {
						t.Fatal(err)
					}
					req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
					req.Header.Set("Origin", tc.origin)
					req.Header.Set("Sec-Fetch-Site", tc.site)
					resp, err := client.Do(req)
					if err != nil {
						t.Fatal(err)
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
					if tc.allowed {
						if resp.StatusCode >= 400 {
							t.Fatalf("allowed request: status %d", resp.StatusCode)
						}
					} else if resp.StatusCode != http.StatusForbidden {
						t.Fatalf("cross-origin request: status %d", resp.StatusCode)
					}
					found := false
					for _, record := range a.inv.Records() {
						if record.Name == name {
							found = true
							if record.Expected != tc.allowed {
								t.Errorf("baseline changed = %v, want %v", record.Expected, tc.allowed)
							}
						}
					}
					if !found {
						t.Fatal("observed device missing")
					}
				})
			}
		})
	}
}

func TestAdminAPIRejectsCrossOriginMutationMethods(t *testing.T) {
	a := startTestApp(t)
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		req, err := http.NewRequest(method, "http://"+a.AdminAddr()+"/api/v1/sources/test", strings.NewReader(`{"id":"test","type":"socketcan","interface":"can0"}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "https://foreign.example")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s: status %d", method, resp.StatusCode)
		}
	}
	if sources, err := a.cfgSvc.ListSources(context.Background()); err != nil || len(sources) != 0 {
		t.Fatalf("rejected writes changed sources: %v, %v", sources, err)
	}
}
