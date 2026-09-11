package api_test

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/model"
)

func TestDurationWritesAndExportImportRoundTrip(t *testing.T) {
	srv, _ := newTestServer(t)
	source := model.Source{ID: "input", Name: "Input", Type: model.SourceSocketCAN, Interface: "can0"}
	mustStatus(t, putEntity(t, srv, "sources", source.ID, source), http.StatusOK)
	for _, s := range []model.Sink{
		{ID: "http", Name: "HTTP", Type: model.SinkHTTPPost, URL: "https://example.com/envelopes", RequestTimeout: model.Duration(15 * time.Second)},
		{ID: "postgres", Name: "PostgreSQL", Type: model.SinkPostgres, URL: "postgres://localhost/beacon", WriteTimeout: model.Duration(20 * time.Second)},
	} {
		mustStatus(t, putEntity(t, srv, "sinks", s.ID, s), http.StatusOK)
		var got model.Sink
		decodeInto(t, doJSON(t, http.MethodGet, srv.URL+"/api/v1/sinks/"+s.ID, nil), &got)
		if !reflect.DeepEqual(got, s) {
			t.Fatalf("sink round trip = %+v, want %+v", got, s)
		}
	}
	connector := model.Connector{ID: "route", Name: "Route", SourceID: source.ID, SinkID: "http", Buffer: model.BufferLimits{MaxAge: model.Duration(24 * time.Hour)}}
	mustStatus(t, putEntity(t, srv, "connectors", connector.ID, connector), http.StatusOK)
	var exported model.Config
	decodeInto(t, doJSON(t, http.MethodGet, srv.URL+"/api/v1/config/export", nil), &exported)
	mustStatus(t, doJSON(t, http.MethodPost, srv.URL+"/api/v1/config/import", exported), http.StatusOK)
	var restored model.Config
	decodeInto(t, doJSON(t, http.MethodGet, srv.URL+"/api/v1/config/export", nil), &restored)
	if !reflect.DeepEqual(exported, restored) {
		t.Fatalf("export/import changed config: %+v -> %+v", exported, restored)
	}
	bad := map[string]any{"id": "http", "name": "HTTP", "type": "http_post", "enabled": false, "url": "https://example.com/envelopes", "request_timeout": 1000000000}
	mustStatus(t, putEntity(t, srv, "sinks", "http", bad), http.StatusUnprocessableEntity)
}

func TestConfigurationSchemasDescribeWireContract(t *testing.T) {
	srv, _ := newTestServer(t)
	var doc struct {
		Components struct {
			Schemas map[string]struct {
				Properties map[string]struct {
					Type        any
					Description string
					Enum        []any
					MaxItems    *int `json:"maxItems"`
				}
				OneOf []json.RawMessage `json:"oneOf"`
			}
		}
	}
	decodeInto(t, doJSON(t, http.MethodGet, srv.URL+"/api/openapi.json", nil), &doc)
	for _, name := range []string{"Source", "Sink", "Connector", "BufferLimits", "Settings", "ResourceConfig", "ObservabilityConfig", "Config"} {
		s := doc.Components.Schemas[name]
		for field, p := range s.Properties {
			if p.Description == "" {
				t.Errorf("%s.%s has no description", name, field)
			}
		}
	}
	for _, field := range []string{"request_timeout", "write_timeout"} {
		if got := doc.Components.Schemas["Sink"].Properties[field].Type; got != "string" {
			t.Errorf("%s type = %q", field, got)
		}
	}
	if doc.Components.Schemas["BufferLimits"].Properties["max_age"].Type != "string" {
		t.Error("max_age must be a string")
	}
	for name, count := range map[string]int{"Source": 8, "Sink": 11} {
		s := doc.Components.Schemas[name]
		if len(s.Properties["type"].Enum) != count || len(s.OneOf) != count {
			t.Errorf("%s missing transport enums/requirements", name)
		}
	}
	if len(doc.Components.Schemas["Connector"].Properties["mode"].Enum) != 4 {
		t.Error("missing bridge policies and empty/default policy")
	}
	if max := doc.Components.Schemas["Config"].Properties["sources"].MaxItems; max == nil || *max != model.MaxSources {
		t.Error("source limit disagrees with model")
	}
}
