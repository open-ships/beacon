package model

import (
	"fmt"
	"net"
	"net/textproto"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
)

var slugRE = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
var httpHeaderNameRE = regexp.MustCompile("^[!#$%&'*+\\-.^_`|~0-9A-Za-z]+$")

func validSlug(id string) error {
	if !slugRE.MatchString(id) {
		return fmt.Errorf("id %q: must match %s", id, slugRE)
	}
	return nil
}

func (s Source) Validate() error {
	if err := validSlug(s.ID); err != nil {
		return fmt.Errorf("source %w", err)
	}
	switch s.Type {
	case SourceSocketCAN:
		if s.Interface == "" {
			return fmt.Errorf("source %q: socketcan requires interface", s.ID)
		}
	case SourceUSBCAN:
		if s.Port == "" {
			return fmt.Errorf("source %q: usbcan requires port", s.ID)
		}
	case SourceHTTPSSE, SourceHTTPWS:
		u, err := url.Parse(s.URL)
		if err != nil || u.Host == "" ||
			(u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "ws" && u.Scheme != "wss") {
			return fmt.Errorf("source %q: invalid url %q", s.ID, s.URL)
		}
	case SourceMQTT:
		if err := validateMQTTBrokerURL(s.URL); err != nil {
			return fmt.Errorf("source %q: invalid mqtt broker url %q: %w", s.ID, s.URL, err)
		}
		if strings.TrimSpace(s.Topic) == "" {
			return fmt.Errorf("source %q: mqtt requires topic", s.ID)
		}
	case SourceFile:
		if !filepath.IsAbs(s.FilePath) && !strings.HasPrefix(s.FilePath, "/") {
			return fmt.Errorf("source %q: file_path must be an absolute path", s.ID)
		}
	case SourceTCP, SourceUDP:
		if _, _, err := net.SplitHostPort(s.Address); err != nil {
			return fmt.Errorf("source %q: invalid gateway address %q: %v", s.ID, s.Address, err)
		}
		if s.Format != StreamFormatYDRaw && s.Format != StreamFormatActisense {
			return fmt.Errorf("source %q: format must be %q or %q", s.ID, StreamFormatYDRaw, StreamFormatActisense)
		}
	default:
		return fmt.Errorf("source %q: unknown type %q", s.ID, s.Type)
	}
	return nil
}

func (s Sink) Validate() error {
	if err := validSlug(s.ID); err != nil {
		return fmt.Errorf("sink %w", err)
	}
	switch s.Type {
	case SinkSocketCAN:
		if s.Interface == "" {
			return fmt.Errorf("sink %q: socketcan requires interface", s.ID)
		}
	case SinkUSBCAN:
		if s.Port == "" {
			return fmt.Errorf("sink %q: usbcan requires port", s.ID)
		}
	case SinkHTTPSSE, SinkHTTPWS:
		if !strings.HasPrefix(s.Path, "/") {
			return fmt.Errorf("sink %q: path must start with /", s.ID)
		}
		for _, p := range ReservedPathPrefixes {
			if s.Path == p || strings.HasPrefix(s.Path, p+"/") {
				return fmt.Errorf("sink %q: path %q is reserved", s.ID, s.Path)
			}
		}
	case SinkHTTPPost:
		u, err := url.Parse(s.URL)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.Fragment != "" {
			return fmt.Errorf("sink %q: invalid HTTP POST url %q (expected http:// or https:// with no fragment)", s.ID, s.URL)
		}
		if u.User != nil {
			return fmt.Errorf("sink %q: HTTP POST url must not contain credentials; use headers for authentication", s.ID)
		}
		if s.BatchSize < 0 || s.BatchSize > MaxHTTPPostBatchSize {
			return fmt.Errorf("sink %q: batch_size must be between 1 and %d, or 0 for the default", s.ID, MaxHTTPPostBatchSize)
		}
		if s.RequestTimeout < 0 {
			return fmt.Errorf("sink %q: request_timeout must not be negative", s.ID)
		}
		seenHeaders := make(map[string]struct{}, len(s.Headers))
		for name, value := range s.Headers {
			trimmedName := strings.TrimSpace(name)
			if trimmedName != name || !httpHeaderNameRE.MatchString(trimmedName) {
				return fmt.Errorf("sink %q: invalid HTTP header name %q", s.ID, name)
			}
			canonical := textproto.CanonicalMIMEHeaderKey(trimmedName)
			if _, duplicate := seenHeaders[canonical]; duplicate {
				return fmt.Errorf("sink %q: duplicate HTTP header %q", s.ID, canonical)
			}
			seenHeaders[canonical] = struct{}{}
			switch canonical {
			case "Host", "Content-Length", "Content-Encoding", "Transfer-Encoding", "Connection", "Idempotency-Key":
				return fmt.Errorf("sink %q: HTTP header %q is managed by Beacon and cannot be configured", s.ID, canonical)
			}
			for _, r := range value {
				if r == '\r' || r == '\n' || (r < 0x20 && r != '\t') || r == 0x7f {
					return fmt.Errorf("sink %q: HTTP header %q contains an invalid control character", s.ID, canonical)
				}
			}
		}
	case SinkTCP:
		if _, _, err := net.SplitHostPort(s.Address); err != nil {
			return fmt.Errorf("sink %q: invalid tcp address %q: %v", s.ID, s.Address, err)
		}
	case SinkTCPGateway:
		if _, _, err := net.SplitHostPort(s.Address); err != nil {
			return fmt.Errorf("sink %q: invalid gateway address %q: %v", s.ID, s.Address, err)
		}
		if s.Format != StreamFormatYDRaw && s.Format != StreamFormatActisense {
			return fmt.Errorf("sink %q: format must be %q or %q", s.ID, StreamFormatYDRaw, StreamFormatActisense)
		}
	case SinkFile:
		if !filepath.IsAbs(s.FilePath) && !strings.HasPrefix(s.FilePath, "/") {
			return fmt.Errorf("sink %q: file_path must be an absolute path", s.ID)
		}
		if s.Format != FileFormatNDJSON && s.Format != FileFormatCANDump {
			return fmt.Errorf("sink %q: format must be %q or %q", s.ID, FileFormatNDJSON, FileFormatCANDump)
		}
		if s.MaxFileBytes < 0 {
			return fmt.Errorf("sink %q: max_file_bytes must not be negative", s.ID)
		}
		if s.MaxFiles < 0 {
			return fmt.Errorf("sink %q: max_files must not be negative", s.ID)
		}
	case SinkMQTT:
		if err := validateMQTTBrokerURL(s.URL); err != nil {
			return fmt.Errorf("sink %q: invalid mqtt broker url %q: %w", s.ID, s.URL, err)
		}
		if strings.TrimSpace(s.Topic) == "" {
			return fmt.Errorf("sink %q: mqtt requires topic", s.ID)
		}
		if strings.ContainsAny(s.Topic, "+#") {
			return fmt.Errorf("sink %q: mqtt publish topic must not contain wildcards", s.ID)
		}
	case SinkNull:
		// Null sinks have no type-specific configuration.
	default:
		return fmt.Errorf("sink %q: unknown type %q", s.ID, s.Type)
	}
	return nil
}

func validateMQTTBrokerURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if u.Host == "" {
		return fmt.Errorf("missing host")
	}
	switch u.Scheme {
	case "tcp", "ssl", "mqtt", "mqtts", "ws", "wss":
		return nil
	default:
		return fmt.Errorf("scheme must be tcp, ssl, mqtt, mqtts, ws, or wss")
	}
}

func NormalizeMQTTBrokerURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	switch u.Scheme {
	case "mqtt":
		u.Scheme = "tcp"
	case "mqtts":
		u.Scheme = "ssl"
	}
	return u.String()
}

func (c Connector) Validate() error {
	if err := validSlug(c.ID); err != nil {
		return fmt.Errorf("connector %w", err)
	}
	if c.SourceID == "" || c.SinkID == "" {
		return fmt.Errorf("connector %q: source_id and sink_id are required", c.ID)
	}
	if c.Buffer.MaxMessages < 0 || c.Buffer.MaxAge < 0 || c.Buffer.MaxBytes < 0 {
		return fmt.Errorf("connector %q: buffer limits must not be negative", c.ID)
	}
	switch c.EffectiveMode() {
	case BridgeSemantic, BridgeTransparent, BridgeObserve:
	default:
		return fmt.Errorf("connector %q: mode must be %q, %q, or %q", c.ID, BridgeSemantic, BridgeTransparent, BridgeObserve)
	}
	if c.ForwardManagement && c.EffectiveMode() != BridgeTransparent {
		return fmt.Errorf("connector %q: forward_management is only valid in transparent mode", c.ID)
	}
	return nil
}

// Validate checks structural rules across the whole config: per-entity
// rules, ID uniqueness, reference integrity, and sink path collisions.
func (c *Config) Validate() error {
	ids := map[string]bool{}
	srcIDs := map[string]bool{}
	for _, s := range c.Sources {
		if err := s.Validate(); err != nil {
			return err
		}
		if ids["src:"+s.ID] {
			return fmt.Errorf("duplicate source id %q", s.ID)
		}
		ids["src:"+s.ID] = true
		srcIDs[s.ID] = true
	}
	sinkIDs := map[string]bool{}
	sinksByID := map[string]Sink{}
	paths := map[string]string{}
	for _, s := range c.Sinks {
		if err := s.Validate(); err != nil {
			return err
		}
		if ids["snk:"+s.ID] {
			return fmt.Errorf("duplicate sink id %q", s.ID)
		}
		ids["snk:"+s.ID] = true
		sinkIDs[s.ID] = true
		sinksByID[s.ID] = s
		if s.Path != "" {
			if other, dup := paths[s.Path]; dup {
				return fmt.Errorf("sinks %q and %q share path %q", other, s.ID, s.Path)
			}
			paths[s.Path] = s.ID
		}
	}
	for _, cn := range c.Connectors {
		if err := cn.Validate(); err != nil {
			return err
		}
		if ids["con:"+cn.ID] {
			return fmt.Errorf("duplicate connector id %q", cn.ID)
		}
		ids["con:"+cn.ID] = true
		if !srcIDs[cn.SourceID] {
			return fmt.Errorf("connector %q: unknown source %q", cn.ID, cn.SourceID)
		}
		if !sinkIDs[cn.SinkID] {
			return fmt.Errorf("connector %q: unknown sink %q", cn.ID, cn.SinkID)
		}
		if cn.EffectiveMode() == BridgeTransparent {
			sink := sinksByID[cn.SinkID]
			if sink.Type != SinkSocketCAN {
				return fmt.Errorf("connector %q: transparent mode currently requires a socketcan sink", cn.ID)
			}
			for _, source := range c.Sources {
				if source.ID == cn.SourceID && source.Type == SourceSocketCAN && source.Interface == sink.Interface {
					return fmt.Errorf("connector %q: transparent source and sink cannot use the same socketcan interface", cn.ID)
				}
			}
		}
	}
	return nil
}
