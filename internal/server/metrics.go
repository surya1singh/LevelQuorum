package server

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// latencyBounds are the upper bounds, in seconds, of the command duration
// histogram. A final bucket catches everything slower.
var latencyBounds = [...]float64{
	0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1,
}

// commandStats counts one command's calls, errors and execution time. The
// time covers running the command, not reading it from or writing its reply
// to the network.
type commandStats struct {
	calls   atomic.Uint64
	errors  atomic.Uint64
	nanos   atomic.Uint64
	buckets [len(latencyBounds) + 1]atomic.Uint64
}

func (st *commandStats) observe(d time.Duration, failed bool) {
	st.calls.Add(1)
	if failed {
		st.errors.Add(1)
	}
	st.nanos.Add(uint64(d))
	secs := d.Seconds()
	i := sort.SearchFloat64s(latencyBounds[:], secs)
	st.buckets[i].Add(1)
}

// unknownCommand is the label under which commands the server does not
// implement are counted.
const unknownCommand = "unknown"

func newStatsTable() map[string]*commandStats {
	table := make(map[string]*commandStats, len(commands)+1)
	for name := range commands {
		table[name] = &commandStats{}
	}
	table[unknownCommand] = &commandStats{}
	return table
}

// totalCommands is the number of commands processed since start.
func (s *Server) totalCommands() uint64 {
	var n uint64
	for _, st := range s.stats {
		n += st.calls.Load()
	}
	return n
}

// MetricsHandler serves the server's metrics in the Prometheus text format.
func (s *Server) MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		s.WriteMetrics(w)
	})
}

// WriteMetrics writes all metrics in the Prometheus text exposition format.
func (s *Server) WriteMetrics(out io.Writer) {
	w := bufio.NewWriter(out)
	defer w.Flush()

	metric := func(name, kind, help string) {
		fmt.Fprintf(w, "# HELP levelquorum_%s %s\n# TYPE levelquorum_%s %s\n", name, help, name, kind)
	}
	sample := func(name, labels string, value float64) {
		if labels != "" {
			labels = "{" + labels + "}"
		}
		fmt.Fprintf(w, "levelquorum_%s%s %s\n", name, labels, strconv.FormatFloat(value, 'g', -1, 64))
	}

	metric("uptime_seconds", "gauge", "Seconds since the server started.")
	sample("uptime_seconds", "", time.Since(s.started).Seconds())
	metric("connected_clients", "gauge", "Open client connections.")
	sample("connected_clients", "", float64(s.clientCount()))

	// Commands that have never been called are left out.
	names := make([]string, 0, len(s.stats))
	for name, st := range s.stats {
		if st.calls.Load() > 0 {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	label := func(name string) string { return `command="` + strings.ToLower(name) + `"` }

	metric("commands_total", "counter", "Commands processed, by command.")
	for _, name := range names {
		sample("commands_total", label(name), float64(s.stats[name].calls.Load()))
	}
	metric("command_errors_total", "counter", "Commands answered with an error, by command.")
	for _, name := range names {
		sample("command_errors_total", label(name), float64(s.stats[name].errors.Load()))
	}
	metric("command_duration_seconds", "histogram", "Time spent executing commands, excluding network I/O, by command.")
	for _, name := range names {
		st := s.stats[name]
		var cumulative uint64
		for i, bound := range latencyBounds {
			cumulative += st.buckets[i].Load()
			sample("command_duration_seconds_bucket", label(name)+`,le="`+strconv.FormatFloat(bound, 'g', -1, 64)+`"`, float64(cumulative))
		}
		cumulative += st.buckets[len(latencyBounds)].Load()
		sample("command_duration_seconds_bucket", label(name)+`,le="+Inf"`, float64(cumulative))
		sample("command_duration_seconds_sum", label(name), float64(st.nanos.Load())/1e9)
		sample("command_duration_seconds_count", label(name), float64(cumulative))
	}

	stats := s.eng.Stats()
	perNamespace := []struct {
		name, kind, help string
		value            func(i int) float64
	}{
		{"namespace_disk_bytes", "gauge", "Size of the namespace's data files.", func(i int) float64 { return float64(stats[i].DiskBytes) }},
		{"namespace_memtable_bytes", "gauge", "Size of the namespace's data not yet written to files.", func(i int) float64 { return float64(stats[i].MemtableBytes) }},
		{"namespace_max_size_bytes", "gauge", "Size at which a cache namespace evicts; 0 for durable namespaces.", func(i int) float64 { return float64(stats[i].Spec.MaxSize) }},
		{"namespace_estimated_keys", "gauge", "RocksDB's estimate of the number of keys; includes copies not yet removed.", func(i int) float64 { return float64(stats[i].EstimatedKeys) }},
		{"namespace_buckets", "gauge", "Column families holding the namespace: 1 for durable, several for cache.", func(i int) float64 { return float64(stats[i].Buckets) }},
		{"namespace_evicted_buckets_total", "counter", "Buckets dropped from a cache namespace to stay within its size limit.", func(i int) float64 { return float64(stats[i].EvictedBuckets) }},
		{"namespace_promotions_total", "counter", "Keys rewritten into the newest bucket because they were read while close to eviction.", func(i int) float64 { return float64(stats[i].Promotions) }},
	}
	for _, m := range perNamespace {
		metric(m.name, m.kind, m.help)
		for i, st := range stats {
			sample(m.name, `namespace="`+st.Spec.Name+`",profile="`+st.Spec.Profile.String()+`"`, m.value(i))
		}
	}

	if hot, ok := s.eng.HotStats(); ok {
		metric("hot_cache_hits_total", "counter", "Reads answered from the RAM tier.")
		sample("hot_cache_hits_total", "", float64(hot.Hits))
		metric("hot_cache_misses_total", "counter", "Reads that had to go to RocksDB.")
		sample("hot_cache_misses_total", "", float64(hot.Misses))
		metric("hot_cache_evictions_total", "counter", "Entries removed from the RAM tier to make room.")
		sample("hot_cache_evictions_total", "", float64(hot.Evictions))
		metric("hot_cache_entries", "gauge", "Entries in the RAM tier.")
		sample("hot_cache_entries", "", float64(hot.Entries))
		metric("hot_cache_bytes", "gauge", "Bytes used by the RAM tier.")
		sample("hot_cache_bytes", "", float64(hot.Bytes))
		metric("hot_cache_capacity_bytes", "gauge", "Configured size of the RAM tier.")
		sample("hot_cache_capacity_bytes", "", float64(hot.Capacity))
	}
	metric("block_cache_bytes", "gauge", "Bytes held by RocksDB's block cache.")
	sample("block_cache_bytes", "", float64(s.eng.BlockCacheUsage()))
}
