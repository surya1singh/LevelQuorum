package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/surya1singh/LevelQuorum/internal/storage"
)

func TestParse(t *testing.T) {
	cfg, err := Parse([]byte(`
listen: "0.0.0.0:7000"
data_dir: /var/lib/levelquorum
block_cache_size: 4GB
hot_cache_size: 0
metrics_listen: "127.0.0.1:9380"
max_bulk_size: 1048576
namespaces:
  - name: sessions
    prefix: "session:"
    profile: cache
    max_size: 2TB
    default_ttl: 7d
  - name: orders
    prefix: "orders:"
    profile: STRICT
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != "0.0.0.0:7000" || cfg.DataDir != "/var/lib/levelquorum" {
		t.Errorf("listen/data_dir = %q, %q", cfg.Listen, cfg.DataDir)
	}
	if cfg.BlockCacheSize != 4<<30 || cfg.MaxBulkSize != 1<<20 {
		t.Errorf("block_cache_size/max_bulk_size = %d, %d", cfg.BlockCacheSize, cfg.MaxBulkSize)
	}
	if cfg.HotCacheSize != 0 || cfg.MetricsListen != "127.0.0.1:9380" {
		t.Errorf("hot_cache_size/metrics_listen = %d, %q; zero must be accepted, it disables the hot tier", cfg.HotCacheSize, cfg.MetricsListen)
	}
	if cfg.WriteBufferSize != Default().WriteBufferSize {
		t.Errorf("write_buffer_size = %d; an omitted setting must keep its default", cfg.WriteBufferSize)
	}

	specs, err := cfg.NamespaceSpecs()
	if err != nil {
		t.Fatal(err)
	}
	want := []storage.NamespaceSpec{
		{Name: "sessions", Prefix: "session:", Profile: storage.Cache, MaxSize: 2 << 40, DefaultTTL: 7 * 24 * time.Hour},
		{Name: "orders", Prefix: "orders:", Profile: storage.Strict},
	}
	if len(specs) != len(want) || specs[0] != want[0] || specs[1] != want[1] {
		t.Errorf("namespaces = %+v; want %+v", specs, want)
	}
}

func TestParseEmptyIsDefault(t *testing.T) {
	for _, in := range []string{"", "# only a comment\n"} {
		cfg, err := Parse([]byte(in))
		if err != nil {
			t.Fatalf("Parse(%q): %v", in, err)
		}
		if cfg.Listen != "127.0.0.1:6380" || len(cfg.Namespaces) != 0 || cfg.HotCacheSize != 128<<20 || cfg.MetricsListen != "" {
			t.Errorf("Parse(%q) = %+v; want the defaults", in, cfg)
		}
	}
}

func TestParseErrors(t *testing.T) {
	for name, in := range map[string]string{
		"unknown field":   "listn: x\n",
		"bad size":        "block_cache_size: lots\n",
		"zero size":       "max_bulk_size: 0\n",
		"huge bulk":       "max_bulk_size: 4GB\n",
		"bad profile":     "namespaces: [{name: a, prefix: 'a:', profile: fast}]\n",
		"bad duration":    "namespaces: [{name: a, prefix: 'a:', profile: safe, default_ttl: soon}]\n",
		"negative ttl":    "namespaces: [{name: a, prefix: 'a:', profile: safe, default_ttl: -5s}]\n",
		"empty listen":    "listen: ''\n",
		"unknown ns key":  "namespaces: [{name: a, prefix: 'a:', profile: safe, maxsize: 1GB}]\n",
		"size overflow":   "block_cache_size: 99999999999TB\n",
		"negative number": "block_cache_size: -1\n",
	} {
		if _, err := Parse([]byte(in)); err == nil {
			t.Errorf("%s: Parse(%q) succeeded; want an error", name, in)
		}
	}
}

func TestParseSizeAndDuration(t *testing.T) {
	for in, want := range map[string]Size{
		"0": 0, "123": 123, "1B": 1, "1KB": 1 << 10, "512mb": 512 << 20, " 10 GB ": 10 << 30, "2TB": 2 << 40,
	} {
		if got, err := ParseSize(in); err != nil || got != want {
			t.Errorf("ParseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for in, want := range map[string]time.Duration{
		"0s": 0, "30s": 30 * time.Second, "90m": 90 * time.Minute, "24h": 24 * time.Hour, "7d": 7 * 24 * time.Hour,
	} {
		if got, err := ParseDuration(in); err != nil || time.Duration(got) != want {
			t.Errorf("ParseDuration(%q) = %v, %v; want %v", in, time.Duration(got), err, want)
		}
	}
	for _, in := range []string{"", "d", "1.5d", "-1d", "5", "999999999999d"} {
		if got, err := ParseDuration(in); err == nil {
			t.Errorf("ParseDuration(%q) = %v; want an error", in, time.Duration(got))
		}
	}
}

// The example file shipped in the repository must stay loadable.
func TestExampleFile(t *testing.T) {
	path := filepath.Join("..", "..", "levelquorum.example.yaml")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Namespaces) == 0 {
		t.Fatal("the example defines no namespaces")
	}

	_, err = Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if !os.IsNotExist(err) {
		t.Errorf("Load of a missing file: err = %v; want not-exist", err)
	}
	bad := filepath.Join(t.TempDir(), "bad.yaml")
	os.WriteFile(bad, []byte("listn: x\n"), 0o644)
	if _, err := Load(bad); err == nil || !strings.Contains(err.Error(), "bad.yaml") {
		t.Errorf("Load of an invalid file: err = %v; want one naming the file", err)
	}
}
