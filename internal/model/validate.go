package model

import (
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"
)

var slugRE = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

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
	case SinkTCP:
		if _, _, err := net.SplitHostPort(s.Address); err != nil {
			return fmt.Errorf("sink %q: invalid tcp address %q: %v", s.ID, s.Address, err)
		}
	default:
		return fmt.Errorf("sink %q: unknown type %q", s.ID, s.Type)
	}
	return nil
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
	}
	return nil
}
