package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Profile is a durability level. It is set per namespace, and a connection
// may raise it for its own writes.
type Profile uint8

const (
	// Cache: acknowledged once applied; data may be evicted when the
	// namespace reaches its size limit.
	Cache Profile = iota
	// Safe: never evicted. In a cluster a write is acknowledged once a
	// majority of replicas have it. On a single node it behaves like Cache
	// without eviction: it survives a process crash but not a power loss.
	Safe
	// Strict: never evicted and fsynced to disk before it is acknowledged.
	Strict
)

func (p Profile) String() string {
	switch p {
	case Cache:
		return "cache"
	case Safe:
		return "safe"
	case Strict:
		return "strict"
	}
	return fmt.Sprintf("profile(%d)", uint8(p))
}

// ParseProfile parses "cache", "safe" or "strict" (case-insensitive).
func ParseProfile(s string) (Profile, error) {
	switch strings.ToLower(s) {
	case "cache":
		return Cache, nil
	case "safe":
		return Safe, nil
	case "strict":
		return Strict, nil
	}
	return 0, fmt.Errorf("unknown profile %q (want cache, safe or strict)", s)
}

// Durable reports whether data under this profile is exempt from eviction.
func (p Profile) Durable() bool { return p != Cache }

// DefaultNamespace is the name of the namespace with the empty prefix, which
// receives every key no other namespace claims.
const DefaultNamespace = "default"

// DefaultCacheSize is the size limit of the default namespace when the
// configuration does not define one.
const DefaultCacheSize = 1 << 30

// NamespaceSpec describes one namespace: the keys it owns (by prefix) and how
// they are stored.
type NamespaceSpec struct {
	Name    string
	Prefix  string
	Profile Profile
	// MaxSize is the on-disk size limit in bytes for a cache namespace; the
	// oldest data is evicted beyond it. It must be zero for durable profiles.
	MaxSize uint64
	// DefaultTTL is applied to writes that do not carry their own expiry.
	// Zero means no default.
	DefaultTTL time.Duration
}

var nameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

func (s NamespaceSpec) validate() error {
	if !nameRE.MatchString(s.Name) {
		return fmt.Errorf("namespace %q: name must match %s", s.Name, nameRE)
	}
	if (s.Name == DefaultNamespace) != (s.Prefix == "") {
		return fmt.Errorf("namespace %q: the empty prefix belongs to the %q namespace and only to it", s.Name, DefaultNamespace)
	}
	if s.Profile > Strict {
		return fmt.Errorf("namespace %q: invalid profile", s.Name)
	}
	if s.Profile == Cache && s.MaxSize == 0 {
		return fmt.Errorf("namespace %q: a cache namespace needs max_size", s.Name)
	}
	if s.Profile.Durable() && s.MaxSize != 0 {
		return fmt.Errorf("namespace %q: max_size is only valid for the cache profile; %s data is never evicted", s.Name, s.Profile)
	}
	if s.DefaultTTL < 0 {
		return fmt.Errorf("namespace %q: default_ttl must not be negative", s.Name)
	}
	return nil
}

// reconcile merges the namespaces already recorded on disk with the ones in
// the configuration. It returns the full set to serve and the names of the
// namespaces that do not exist on disk yet.
//
// Stored namespaces that the configuration no longer mentions are kept: their
// data is still on disk and must stay reachable. A change that would move
// existing keys (a new prefix) or change whether data can be evicted (cache
// to durable or back) is refused.
func reconcile(stored, configured []NamespaceSpec) (merged []NamespaceSpec, created []string, err error) {
	byName := make(map[string]int, len(stored)+len(configured))
	for _, s := range stored {
		byName[s.Name] = len(merged)
		merged = append(merged, s)
	}

	seen := make(map[string]bool, len(configured))
	for _, c := range configured {
		if err := c.validate(); err != nil {
			return nil, nil, err
		}
		if seen[c.Name] {
			return nil, nil, fmt.Errorf("namespace %q is defined twice", c.Name)
		}
		seen[c.Name] = true

		i, exists := byName[c.Name]
		if !exists {
			byName[c.Name] = len(merged)
			merged = append(merged, c)
			created = append(created, c.Name)
			continue
		}
		old := merged[i]
		if old.Prefix != c.Prefix {
			return nil, nil, fmt.Errorf("namespace %q: prefix cannot change from %q to %q, existing keys would become unreachable", c.Name, old.Prefix, c.Prefix)
		}
		if old.Profile.Durable() != c.Profile.Durable() {
			return nil, nil, fmt.Errorf("namespace %q: profile cannot change from %s to %s, that would change whether existing data can be evicted", c.Name, old.Profile, c.Profile)
		}
		merged[i] = c
	}

	if _, ok := byName[DefaultNamespace]; !ok {
		merged = append(merged, NamespaceSpec{Name: DefaultNamespace, Profile: Cache, MaxSize: DefaultCacheSize})
		created = append(created, DefaultNamespace)
	}

	prefixes := make(map[string]string, len(merged))
	for _, s := range merged {
		if other, dup := prefixes[s.Prefix]; dup {
			return nil, nil, fmt.Errorf("namespaces %q and %q both claim prefix %q", other, s.Name, s.Prefix)
		}
		prefixes[s.Prefix] = s.Name
	}
	return merged, created, nil
}

// sortByPrefixLen orders namespaces longest prefix first, so that the first
// match while routing a key is the most specific one.
func sortByPrefixLen[T any](items []T, prefix func(T) string) {
	sort.SliceStable(items, func(i, j int) bool {
		return len(prefix(items[i])) > len(prefix(items[j]))
	})
}

// The manifest records which namespaces exist. It lives beside the database
// rather than inside it because RocksDB needs each column family's options
// before the database can be opened.
const manifestFile = "NAMESPACES.json"

type manifestEntry struct {
	Name         string `json:"name"`
	Prefix       string `json:"prefix"`
	Profile      string `json:"profile"`
	MaxSize      uint64 `json:"max_size,omitempty"`
	DefaultTTLMs int64  `json:"default_ttl_ms,omitempty"`
}

type manifest struct {
	Version    int             `json:"version"`
	Namespaces []manifestEntry `json:"namespaces"`
}

func loadManifest(dir string) ([]NamespaceSpec, error) {
	raw, err := os.ReadFile(filepath.Join(dir, manifestFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var m manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("%s: %w", manifestFile, err)
	}
	if m.Version != 1 {
		return nil, fmt.Errorf("%s: unsupported version %d", manifestFile, m.Version)
	}
	specs := make([]NamespaceSpec, 0, len(m.Namespaces))
	for _, e := range m.Namespaces {
		p, err := ParseProfile(e.Profile)
		if err != nil {
			return nil, fmt.Errorf("%s: namespace %q: %w", manifestFile, e.Name, err)
		}
		s := NamespaceSpec{
			Name:       e.Name,
			Prefix:     e.Prefix,
			Profile:    p,
			MaxSize:    e.MaxSize,
			DefaultTTL: time.Duration(e.DefaultTTLMs) * time.Millisecond,
		}
		if err := s.validate(); err != nil {
			return nil, fmt.Errorf("%s: %w", manifestFile, err)
		}
		specs = append(specs, s)
	}
	return specs, nil
}

// saveManifest replaces the manifest atomically: write a temporary file,
// fsync it, rename it over the old one, fsync the directory.
func saveManifest(dir string, specs []NamespaceSpec) error {
	m := manifest{Version: 1}
	for _, s := range specs {
		m.Namespaces = append(m.Namespaces, manifestEntry{
			Name:         s.Name,
			Prefix:       s.Prefix,
			Profile:      s.Profile.String(),
			MaxSize:      s.MaxSize,
			DefaultTTLMs: s.DefaultTTL.Milliseconds(),
		})
	}
	sort.Slice(m.Namespaces, func(i, j int) bool { return m.Namespaces[i].Name < m.Namespaces[j].Name })
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}

	path := filepath.Join(dir, manifestFile)
	tmp, err := os.CreateTemp(dir, manifestFile+".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(append(raw, '\n')); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
