// Package config loads the server's YAML configuration file.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/surya1singh/LevelQuorum/internal/storage"
)

// Config is the content of the configuration file. See
// levelquorum.example.yaml for a commented example.
type Config struct {
	// Listen is the TCP address to serve the Redis protocol on.
	Listen string `yaml:"listen"`
	// DataDir holds the database.
	DataDir string `yaml:"data_dir"`
	// BlockCacheSize is the RAM for RocksDB's cache of data blocks.
	BlockCacheSize Size `yaml:"block_cache_size"`
	// WriteBufferSize is the in-memory write buffer per namespace.
	WriteBufferSize Size `yaml:"write_buffer_size"`
	// MaxBulkSize is the largest key or value a client may send.
	MaxBulkSize Size `yaml:"max_bulk_size"`
	// Namespaces maps key prefixes to storage profiles.
	Namespaces []Namespace `yaml:"namespaces"`
}

// Namespace is one entry of the namespaces list.
type Namespace struct {
	Name       string   `yaml:"name"`
	Prefix     string   `yaml:"prefix"`
	Profile    string   `yaml:"profile"`
	MaxSize    Size     `yaml:"max_size"`
	DefaultTTL Duration `yaml:"default_ttl"`
}

// Default returns the configuration used when no file is given: listen on
// localhost only, store data in ./data, and serve a single cache namespace.
func Default() Config {
	return Config{
		Listen:          "127.0.0.1:6380",
		DataDir:         "data",
		BlockCacheSize:  256 << 20,
		WriteBufferSize: 64 << 20,
		MaxBulkSize:     64 << 20,
	}
}

// Load reads the file at path on top of the defaults.
func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	cfg, err := Parse(raw)
	if err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

// Parse decodes YAML on top of the defaults. Unknown fields are errors, so a
// misspelt setting cannot be silently ignored.
func Parse(raw []byte) (Config, error) {
	cfg := Default()
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks the settings that do not need the database to verify.
func (c Config) Validate() error {
	if c.Listen == "" {
		return errors.New("listen must not be empty")
	}
	if c.DataDir == "" {
		return errors.New("data_dir must not be empty")
	}
	if c.BlockCacheSize == 0 || c.WriteBufferSize == 0 || c.MaxBulkSize == 0 {
		return errors.New("block_cache_size, write_buffer_size and max_bulk_size must be greater than zero")
	}
	if c.MaxBulkSize > math.MaxInt32 {
		return errors.New("max_bulk_size must be below 2GB")
	}
	_, err := c.NamespaceSpecs()
	return err
}

// NamespaceSpecs converts the namespaces list for the storage engine.
func (c Config) NamespaceSpecs() ([]storage.NamespaceSpec, error) {
	specs := make([]storage.NamespaceSpec, 0, len(c.Namespaces))
	for _, n := range c.Namespaces {
		p, err := storage.ParseProfile(n.Profile)
		if err != nil {
			return nil, fmt.Errorf("namespace %q: %w", n.Name, err)
		}
		specs = append(specs, storage.NamespaceSpec{
			Name:       n.Name,
			Prefix:     n.Prefix,
			Profile:    p,
			MaxSize:    uint64(n.MaxSize),
			DefaultTTL: time.Duration(n.DefaultTTL),
		})
	}
	return specs, nil
}

// Size is a number of bytes, written in YAML as a plain number or with a
// unit: 512MB, 10GB, 2TB. Units are powers of 1024.
type Size uint64

var sizeUnits = []struct {
	suffix string
	factor uint64
}{
	{"TB", 1 << 40}, {"GB", 1 << 30}, {"MB", 1 << 20}, {"KB", 1 << 10}, {"B", 1},
}

// ParseSize parses a Size.
func ParseSize(s string) (Size, error) {
	text := strings.ToUpper(strings.TrimSpace(s))
	factor := uint64(1)
	for _, u := range sizeUnits {
		if strings.HasSuffix(text, u.suffix) {
			text = strings.TrimSpace(strings.TrimSuffix(text, u.suffix))
			factor = u.factor
			break
		}
	}
	n, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q (examples: 1048576, 512MB, 10GB, 2TB)", s)
	}
	if n > math.MaxUint64/factor {
		return 0, fmt.Errorf("size %q is too large", s)
	}
	return Size(n * factor), nil
}

func (s *Size) UnmarshalYAML(node *yaml.Node) error {
	v, err := ParseSize(node.Value)
	if err != nil {
		return fmt.Errorf("line %d: %w", node.Line, err)
	}
	*s = v
	return nil
}

// Duration is a time.Duration written in YAML as 30s, 15m, 24h or 7d.
type Duration time.Duration

// ParseDuration parses a Duration. Besides Go's units it accepts whole days,
// such as 7d.
func ParseDuration(s string) (Duration, error) {
	text := strings.TrimSpace(s)
	if days, ok := strings.CutSuffix(text, "d"); ok {
		n, err := strconv.ParseInt(days, 10, 64)
		if err != nil || n < 0 || n > math.MaxInt64/int64(24*time.Hour) {
			return 0, fmt.Errorf("invalid duration %q (examples: 30s, 15m, 24h, 7d)", s)
		}
		return Duration(time.Duration(n) * 24 * time.Hour), nil
	}
	d, err := time.ParseDuration(text)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("invalid duration %q (examples: 30s, 15m, 24h, 7d)", s)
	}
	return Duration(d), nil
}

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	v, err := ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("line %d: %w", node.Line, err)
	}
	*d = v
	return nil
}
