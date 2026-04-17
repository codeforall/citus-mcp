// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Alarms framework (B11). A shared, in-memory sink that any diagnostic tool
// can emit findings into, surfaced via citus_alarms_list / citus_alarms_ack
// tools and the citus://alarms resource. All emissions MUST go through this
// sink so early-warnings accumulate in one place instead of being scattered
// across tool outputs.

package diagnostics

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Severity classifies alarm urgency.
type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// severityRank gives a total order for filtering "at least this severe".
func severityRank(s Severity) int {
	switch s {
	case SeverityCritical:
		return 2
	case SeverityWarning:
		return 1
	default:
		return 0
	}
}

// Alarm is a single finding emitted by a diagnostic tool. It is deliberately
// small, JSON-friendly, and free of user row data. `Evidence` should contain
// aggregated numbers / catalog identifiers only.
type Alarm struct {
	ID        string         `json:"id"`
	Kind      string         `json:"kind"`     // stable machine key, e.g. "metadata_cache.growth"
	Severity  Severity       `json:"severity"` // info | warning | critical
	Source    string         `json:"source"`   // tool name that emitted
	Node      string         `json:"node,omitempty"`
	Object    string         `json:"object,omitempty"` // e.g. "public.events"
	Message   string         `json:"message"`          // short human summary
	Evidence  map[string]any `json:"evidence,omitempty"`
	FixHint   string         `json:"fix_hint,omitempty"`
	DocsURL   string         `json:"docs_url,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	SeenCount int            `json:"seen_count"`
	Acked     bool           `json:"acked"`
	AckedAt   *time.Time     `json:"acked_at,omitempty"`
	AckedBy   string         `json:"acked_by,omitempty"`
}

// Filter narrows which alarms are returned by List.
type Filter struct {
	MinSeverity  Severity
	Kind         string
	Source       string
	Node         string
	Object       string
	Since        *time.Time
	IncludeAcked bool
	Limit        int
}

// Sink is the single point of emission + querying for alarms.
type Sink struct {
	mu       sync.RWMutex
	alarms   map[string]*Alarm // keyed by Alarm.ID (deterministic fingerprint)
	maxSize  int
	clock    func() time.Time
}

// NewSink builds an in-memory sink. maxSize caps retained alarms (0 = 1024).
func NewSink(maxSize int) *Sink {
	if maxSize <= 0 {
		maxSize = 1024
	}
	return &Sink{alarms: make(map[string]*Alarm), maxSize: maxSize, clock: time.Now}
}

// Fingerprint returns a stable ID for an alarm based on the dimensions that
// make it "the same finding". Emitting the same alarm twice updates the
// existing entry (bumps SeenCount + UpdatedAt) rather than creating duplicates.
func Fingerprint(a Alarm) string {
	h := sha1.New()
	fmt.Fprintf(h, "%s|%s|%s|%s|%s", a.Kind, a.Source, a.Node, a.Object, a.Severity)
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// Emit adds or updates an alarm. Returns the stored alarm (with ID populated).
// Safe for concurrent use.
func (s *Sink) Emit(a Alarm) *Alarm {
	if s == nil {
		return &a
	}
	if a.Severity == "" {
		a.Severity = SeverityInfo
	}
	now := s.clock()
	a.ID = Fingerprint(a)

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.alarms[a.ID]; ok {
		existing.SeenCount++
		existing.UpdatedAt = now
		// refresh mutable fields that may have changed wording/evidence
		existing.Message = a.Message
		existing.Evidence = a.Evidence
		existing.FixHint = a.FixHint
		existing.DocsURL = a.DocsURL
		// if a previously-acked alarm fires again, un-ack it so the user sees
		// the condition returned.
		existing.Acked = false
		existing.AckedAt = nil
		existing.AckedBy = ""
		return existing
	}

	a.CreatedAt = now
	a.UpdatedAt = now
	a.SeenCount = 1
	s.alarms[a.ID] = &a

	// simple cap: if over maxSize, drop oldest acked first, then oldest overall.
	if len(s.alarms) > s.maxSize {
		s.evictLocked()
	}
	return &a
}

func (s *Sink) evictLocked() {
	type kv struct {
		id    string
		alarm *Alarm
	}
	all := make([]kv, 0, len(s.alarms))
	for id, al := range s.alarms {
		all = append(all, kv{id, al})
	}
	// prefer evicting acked alarms first; within a group, oldest UpdatedAt first.
	sort.Slice(all, func(i, j int) bool {
		if all[i].alarm.Acked != all[j].alarm.Acked {
			return all[i].alarm.Acked
		}
		return all[i].alarm.UpdatedAt.Before(all[j].alarm.UpdatedAt)
	})
	for _, e := range all {
		if len(s.alarms) <= s.maxSize {
			break
		}
		delete(s.alarms, e.id)
	}
}

// List returns a copy of matching alarms sorted by (severity desc, updated_at desc).
func (s *Sink) List(f Filter) []Alarm {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Alarm, 0, len(s.alarms))
	minRank := severityRank(f.MinSeverity)
	for _, a := range s.alarms {
		if !f.IncludeAcked && a.Acked {
			continue
		}
		if f.MinSeverity != "" && severityRank(a.Severity) < minRank {
			continue
		}
		if f.Kind != "" && a.Kind != f.Kind {
			continue
		}
		if f.Source != "" && a.Source != f.Source {
			continue
		}
		if f.Node != "" && a.Node != f.Node {
			continue
		}
		if f.Object != "" && a.Object != f.Object {
			continue
		}
		if f.Since != nil && a.UpdatedAt.Before(*f.Since) {
			continue
		}
		out = append(out, *a) // copy
	}
	sort.Slice(out, func(i, j int) bool {
		ri := severityRank(out[i].Severity)
		rj := severityRank(out[j].Severity)
		if ri != rj {
			return ri > rj
		}
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out
}

// Ack marks one alarm as acknowledged. Returns true if found.
func (s *Sink) Ack(id, by string) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.alarms[id]
	if !ok {
		return false
	}
	now := s.clock()
	a.Acked = true
	a.AckedAt = &now
	a.AckedBy = strings.TrimSpace(by)
	return true
}

// AckMatching acknowledges all alarms matching the filter. Returns count.
func (s *Sink) AckMatching(f Filter, by string) int {
	if s == nil {
		return 0
	}
	// include acked = false always when ack-matching (don't re-ack)
	f.IncludeAcked = false
	matches := s.List(f)
	n := 0
	for _, m := range matches {
		if s.Ack(m.ID, by) {
			n++
		}
	}
	return n
}

// Clear removes all alarms. Intended for tests and for explicit user reset.
func (s *Sink) Clear() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.alarms = make(map[string]*Alarm)
}

// Snapshot returns a stable snapshot (all alarms) for the citus://alarms
// resource. Acked alarms are included; clients filter client-side if needed.
func (s *Sink) Snapshot() []Alarm {
	return s.List(Filter{IncludeAcked: true})
}

// Stats returns aggregate counts by severity for quick UI rendering.
func (s *Sink) Stats() map[string]int {
	if s == nil {
		return map[string]int{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	stats := map[string]int{
		"total":    0,
		"critical": 0,
		"warning":  0,
		"info":     0,
		"acked":    0,
	}
	for _, a := range s.alarms {
		stats["total"]++
		if a.Acked {
			stats["acked"]++
			continue
		}
		stats[string(a.Severity)]++
	}
	return stats
}
