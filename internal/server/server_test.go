package server

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/surya1singh/LevelQuorum/internal/storage"
)

// testClock is a manually advanced clock shared with the engine.
type testClock struct{ ms atomic.Int64 }

func (c *testClock) Now() time.Time          { return time.UnixMilli(c.ms.Load()) }
func (c *testClock) Advance(d time.Duration) { c.ms.Add(d.Milliseconds()) }

type harness struct {
	t     *testing.T
	addr  string
	clock *testClock
	srv   *Server
	eng   *storage.Engine
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func start(t *testing.T) *harness {
	t.Helper()
	clock := &testClock{}
	clock.ms.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli())

	eng, err := storage.Open(storage.Options{
		Dir:             t.TempDir(),
		WriteBufferSize: 1 << 20,
		BlockCacheSize:  8 << 20,
		HotCacheSize:    16 << 20, // on, as in the default configuration
		Now:             clock.Now,
		Logger:          quiet(),
		Namespaces: []storage.NamespaceSpec{
			{Name: "sessions", Prefix: "session:", Profile: storage.Cache, MaxSize: 64 << 20, DefaultTTL: time.Hour},
			{Name: "orders", Prefix: "orders:", Profile: storage.Strict},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := New(eng, Options{MaxBulkSize: 4 << 20, Logger: quiet()})
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()

	t.Cleanup(func() {
		srv.Shutdown()
		if err := <-done; err != nil {
			t.Errorf("Serve returned %v after Shutdown; want nil", err)
		}
		if err := eng.Close(); err != nil {
			t.Errorf("engine Close: %v", err)
		}
	})
	return &harness{t: t, addr: ln.Addr().String(), clock: clock, srv: srv, eng: eng}
}

// conn is a minimal RESP client.
type conn struct {
	t  *testing.T
	nc net.Conn
	br *bufio.Reader
}

func (h *harness) dial() *conn {
	h.t.Helper()
	nc, err := net.Dial("tcp", h.addr)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { nc.Close() })
	nc.SetDeadline(time.Now().Add(30 * time.Second))
	return &conn{t: h.t, nc: nc, br: bufio.NewReader(nc)}
}

func encode(args ...string) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	return b.Bytes()
}

// respErr is an error reply.
type respErr string

// read parses one reply into nil, string (simple or bulk), int64, respErr or
// []any.
func (c *conn) read() any {
	c.t.Helper()
	line, err := c.br.ReadString('\n')
	if err != nil {
		c.t.Fatalf("reading reply: %v", err)
	}
	if !strings.HasSuffix(line, "\r\n") {
		c.t.Fatalf("reply line %q does not end in CRLF", line)
	}
	body := line[1 : len(line)-2]
	switch line[0] {
	case '+':
		return body
	case '-':
		return respErr(body)
	case ':':
		n, err := strconv.ParseInt(body, 10, 64)
		if err != nil {
			c.t.Fatalf("bad integer reply %q", line)
		}
		return n
	case '$':
		n, _ := strconv.Atoi(body)
		if n < 0 {
			return nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.br, buf); err != nil {
			c.t.Fatalf("reading bulk: %v", err)
		}
		if string(buf[n:]) != "\r\n" {
			c.t.Fatalf("bulk reply is not terminated by CRLF")
		}
		return string(buf[:n])
	case '*':
		n, _ := strconv.Atoi(body)
		out := make([]any, n)
		for i := range out {
			out[i] = c.read()
		}
		return out
	}
	c.t.Fatalf("unknown reply type in %q", line)
	return nil
}

func (c *conn) do(args ...string) any {
	c.t.Helper()
	if _, err := c.nc.Write(encode(args...)); err != nil {
		c.t.Fatalf("writing %q: %v", args, err)
	}
	return c.read()
}

// want runs a command and checks its reply.
func (c *conn) want(want any, args ...string) {
	c.t.Helper()
	if got := c.do(args...); !reflect.DeepEqual(got, want) {
		c.t.Fatalf("%q = %#v; want %#v", args, got, want)
	}
}

// wantErr runs a command and checks that the reply is an error containing
// substr.
func (c *conn) wantErr(substr string, args ...string) {
	c.t.Helper()
	got := c.do(args...)
	if e, ok := got.(respErr); !ok || !strings.Contains(string(e), substr) {
		c.t.Fatalf("%q = %#v; want an error containing %q", args, got, substr)
	}
}

const i64 = int64(0) // for readable integer literals: i64+3

func TestStrings(t *testing.T) {
	c := start(t).dial()

	c.want("PONG", "PING")
	c.want("hi", "ping", "hi")
	c.want("hello", "ECHO", "hello")

	c.want(nil, "GET", "k")
	c.want("OK", "SET", "k", "v")
	c.want("v", "GET", "k")
	c.want("v", "get", "k") // command names are case-insensitive
	c.want(i64+1, "STRLEN", "k")
	c.want(i64+0, "STRLEN", "missing")
	c.want("string", "TYPE", "k")
	c.want("none", "TYPE", "missing")

	c.want("OK", "SET", "empty", "")
	c.want("", "GET", "empty")
	c.want(i64+1, "EXISTS", "empty")

	c.want(i64+0, "SETNX", "k", "other")
	c.want(i64+1, "SETNX", "k2", "v2")
	c.want("v", "GETSET", "k", "w")
	c.want(nil, "GETSET", "fresh", "x")
	c.want("w", "GETDEL", "k")
	c.want(nil, "GETDEL", "k")

	c.want(i64+3, "APPEND", "a", "foo")
	c.want(i64+6, "APPEND", "a", "bar")
	c.want("foobar", "GET", "a")

	c.want("OK", "MSET", "m1", "1", "m2", "2")
	c.want([]any{"1", nil, "2"}, "MGET", "m1", "nope", "m2")
	c.wantErr("wrong number of arguments for 'mset'", "MSET", "m1", "1", "m2")

	c.want(i64+3, "EXISTS", "m1", "m1", "m2", "nope") // duplicates count each time
	c.want(i64+2, "DEL", "m1", "m1", "m2", "nope")    // but a key is only deleted once
	c.want(i64+0, "UNLINK", "m1")
}

func TestSetOptions(t *testing.T) {
	h := start(t)
	c := h.dial()

	c.want(nil, "SET", "k", "v", "XX")
	c.want("OK", "SET", "k", "v", "NX")
	c.want(nil, "SET", "k", "v2", "nx")
	c.want("OK", "SET", "k", "v3", "XX", "EX", "100")
	c.want(i64+100, "TTL", "k")
	c.want("OK", "SET", "k", "v4", "KEEPTTL")
	c.want(i64+100, "TTL", "k")
	c.want("v4", "SET", "k", "v5", "GET")
	c.want(i64-1, "TTL", "k") // a plain SET clears the expiry
	c.want(nil, "SET", "new", "v", "GET")
	c.want("v", "GET", "new")
	c.want("v", "SET", "new", "w", "NX", "GET") // not written, but the old value is returned
	c.want("v", "GET", "new")

	c.want("OK", "SET", "px", "v", "PX", "1500")
	c.want(i64+1500, "PTTL", "px")
	at := h.clock.Now().Add(50 * time.Second)
	c.want("OK", "SET", "exat", "v", "EXAT", fmt.Sprint(at.Unix()))
	c.want(i64+50, "TTL", "exat")
	c.want("OK", "SET", "pxat", "v", "PXAT", fmt.Sprint(at.UnixMilli()))
	c.want(i64+50000, "PTTL", "pxat")

	// An absolute expiry that is already past leaves no key.
	c.want("OK", "SET", "exat", "v", "EXAT", "1")
	c.want(nil, "GET", "exat")

	for _, bad := range [][]string{
		{"SET", "k", "v", "NX", "XX"},
		{"SET", "k", "v", "EX"},
		{"SET", "k", "v", "EX", "10", "PX", "10"},
		{"SET", "k", "v", "EX", "10", "KEEPTTL"},
		{"SET", "k", "v", "BOGUS"},
	} {
		c.wantErr("syntax error", bad...)
	}
	c.wantErr("not an integer", "SET", "k", "v", "EX", "soon")
	c.wantErr("invalid expire time in 'set'", "SET", "k", "v", "EX", "0")
	c.wantErr("invalid expire time in 'set'", "SET", "k", "v", "PX", "-5")
	c.wantErr("invalid expire time in 'set'", "SET", "k", "v", "EX", "9223372036854775807")
	c.want("v5", "GET", "k") // none of the rejected commands wrote anything
}

func TestExpiry(t *testing.T) {
	h := start(t)
	c := h.dial()

	c.want(i64-2, "TTL", "k")
	c.want(i64-2, "PTTL", "k")
	c.want(i64+0, "EXPIRE", "k", "10")
	c.want("OK", "SET", "k", "v")
	c.want(i64-1, "TTL", "k")
	c.want(i64-1, "EXPIRETIME", "k")
	c.want(i64+0, "PERSIST", "k")

	c.want(i64+1, "EXPIRE", "k", "10")
	c.want(i64+10, "TTL", "k")
	c.want(i64+10000, "PTTL", "k")
	c.want(h.clock.Now().Unix()+10, "EXPIRETIME", "k")
	c.want(h.clock.Now().UnixMilli()+10000, "PEXPIRETIME", "k")

	// TTL rounds to the nearest second, as Redis does.
	h.clock.Advance(400 * time.Millisecond)
	c.want(i64+10, "TTL", "k")
	h.clock.Advance(200 * time.Millisecond)
	c.want(i64+9, "TTL", "k")

	c.want(i64+1, "PERSIST", "k")
	c.want(i64-1, "TTL", "k")

	c.want(i64+1, "PEXPIRE", "k", "250")
	h.clock.Advance(249 * time.Millisecond)
	c.want("v", "GET", "k")
	h.clock.Advance(time.Millisecond)
	c.want(nil, "GET", "k")
	c.want(i64+0, "EXISTS", "k")
	c.want(i64-2, "TTL", "k")

	c.want("OK", "SETEX", "se", "5", "v")
	c.want(i64+5, "TTL", "se")
	c.want("OK", "PSETEX", "pse", "5000", "v")
	c.want(i64+5000, "PTTL", "pse")
	c.wantErr("invalid expire time in 'setex'", "SETEX", "se", "0", "v")
	c.wantErr("invalid expire time in 'psetex'", "PSETEX", "se", "-1", "v")

	// EXPIREAT takes whole seconds; put the clock, which is 850 ms into a
	// second by now, back on a second boundary so the arithmetic is exact.
	h.clock.Advance(150 * time.Millisecond)
	at := h.clock.Now().Add(30 * time.Second)
	c.want("OK", "SET", "a", "v")
	c.want(i64+1, "EXPIREAT", "a", fmt.Sprint(at.Unix()))
	c.want(i64+30, "TTL", "a")
	c.want(i64+1, "PEXPIREAT", "a", fmt.Sprint(at.UnixMilli()+1000))
	c.want(i64+31, "TTL", "a")

	// A non-positive EXPIRE deletes the key.
	c.want(i64+1, "EXPIRE", "a", "-1")
	c.want(nil, "GET", "a")

	c.wantErr("not an integer", "EXPIRE", "se", "soon")
	c.wantErr("invalid expire time in 'expire'", "EXPIRE", "se", "9223372036854775807")
	c.wantErr("not supported", "EXPIRE", "se", "10", "NX")

	// The sessions namespace has a default TTL of one hour.
	c.want("OK", "SET", "session:1", "v")
	c.want(i64+3600, "TTL", "session:1")
	c.want("OK", "SET", "session:2", "v", "EX", "5")
	c.want(i64+5, "TTL", "session:2")
}

func TestCounters(t *testing.T) {
	c := start(t).dial()

	c.want(i64+1, "INCR", "n")
	c.want(i64+11, "INCRBY", "n", "10")
	c.want(i64+10, "DECR", "n")
	c.want(i64-10, "DECRBY", "n", "20")
	c.want("-10", "GET", "n")

	c.want(i64+1, "EXPIRE", "n", "100")
	c.want(i64-9, "INCR", "n")
	c.want(i64+100, "TTL", "n") // counters keep their expiry

	c.want("OK", "SET", "s", "abc")
	c.wantErr("not an integer", "INCR", "s")
	c.wantErr("not an integer", "INCRBY", "n", "1.5")
	c.want("OK", "SET", "big", "9223372036854775807")
	c.wantErr("would overflow", "INCR", "big")
	c.wantErr("would overflow", "DECRBY", "n", "-9223372036854775808")
	c.want("9223372036854775807", "GET", "big")
}

// Counters stay exact when many connections increment them at once.
func TestConcurrentClients(t *testing.T) {
	h := start(t)
	const clients, perClient = 8, 200

	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		c := h.dial()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perClient; j++ {
				if _, ok := c.do("INCR", "orders:counter").(int64); !ok {
					t.Error("INCR did not return an integer")
					return
				}
			}
		}()
	}
	wg.Wait()
	h.dial().want(fmt.Sprint(clients*perClient), "GET", "orders:counter")
}

func TestBinarySafeAndLargeValues(t *testing.T) {
	c := start(t).dial()

	key := "k\x00\r\n\xff"
	val := "v\x00\r\n\xff\x00"
	c.want("OK", "SET", key, val)
	c.want(val, "GET", key)

	big := make([]byte, 3<<20)
	rand.Read(big)
	c.want("OK", "SET", "big", string(big))
	if got := c.do("GET", "big"); got != string(big) {
		t.Fatalf("a 3 MiB value did not round-trip")
	}
	c.want(int64(len(big)), "STRLEN", "big")
}

// Many commands sent in one write are answered in order.
func TestPipelining(t *testing.T) {
	c := start(t).dial()
	const n = 500

	var batch bytes.Buffer
	for i := 0; i < n; i++ {
		batch.Write(encode("SET", fmt.Sprintf("p%d", i), fmt.Sprint(i)))
		batch.Write(encode("GET", fmt.Sprintf("p%d", i)))
	}
	batch.WriteString("\r\n") // a blank line between commands is ignored
	batch.Write(encode("PING"))
	if _, err := c.nc.Write(batch.Bytes()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if got := c.read(); got != "OK" {
			t.Fatalf("reply to SET %d = %#v", i, got)
		}
		if got := c.read(); got != fmt.Sprint(i) {
			t.Fatalf("reply to GET %d = %#v", i, got)
		}
	}
	if got := c.read(); got != "PONG" {
		t.Fatalf("final reply = %#v; want PONG", got)
	}
}

func TestInlineCommands(t *testing.T) {
	c := start(t).dial()
	fmt.Fprint(c.nc, "PING\r\nSET inline value\nGET inline\r\n")
	for _, want := range []any{"PONG", "OK", "value"} {
		if got := c.read(); got != want {
			t.Fatalf("inline reply = %#v; want %#v", got, want)
		}
	}
}

func TestErrors(t *testing.T) {
	c := start(t).dial()
	c.wantErr("unknown command 'FLUSHALL'", "FLUSHALL")
	c.wantErr("with args beginning with: 'a' 'b'", "NOPE", "a", "b")
	c.wantErr("wrong number of arguments for 'get'", "GET")
	c.wantErr("wrong number of arguments for 'get'", "GET", "a", "b")
	c.wantErr("wrong number of arguments for 'set'", "SET", "a")
	c.wantErr("wrong number of arguments for 'ping'", "PING", "a", "b")
	c.want("PONG", "PING") // errors do not close the connection
}

func (c *conn) wantClosed() {
	c.t.Helper()
	if _, err := c.br.ReadByte(); err != io.EOF {
		c.t.Fatalf("read after the expected close: err = %v; want EOF", err)
	}
}

// Malformed protocol gets an error reply and the connection is closed.
func TestProtocolErrors(t *testing.T) {
	h := start(t)
	for name, input := range map[string]string{
		"bad array length": "*abc\r\n",
		"missing $":        "*1\r\n+PING\r\n",
		"oversized bulk":   fmt.Sprintf("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$%d\r\n", 5<<20),
	} {
		c := h.dial()
		fmt.Fprint(c.nc, input)
		if e, ok := c.read().(respErr); !ok || !strings.Contains(string(e), "Protocol error") {
			t.Errorf("%s: reply = %#v; want a protocol error", name, e)
		}
		c.wantClosed()
	}
	h.dial().want("PONG", "PING") // the server itself is unaffected
}

func TestConnectionCommands(t *testing.T) {
	c := start(t).dial()

	c.want("OK", "SELECT", "0")
	c.wantErr("out of range", "SELECT", "1")
	c.wantErr("no password is set", "AUTH", "secret")

	hello, ok := c.do("HELLO").([]any)
	if !ok || len(hello) != 14 || hello[0] != "server" || hello[1] != "levelquorum" || hello[5] != i64+2 {
		t.Fatalf("HELLO = %#v", hello)
	}
	c.wantErr("NOPROTO", "HELLO", "3")
	if _, ok := c.do("HELLO", "2", "SETNAME", "app").([]any); !ok {
		t.Fatal("HELLO 2 SETNAME did not succeed")
	}
	c.want("app", "CLIENT", "GETNAME")
	c.want("OK", "CLIENT", "SETNAME", "worker")
	c.want("worker", "CLIENT", "GETNAME")
	c.want("OK", "CLIENT", "SETINFO", "lib-name", "test")
	if id, ok := c.do("CLIENT", "ID").(int64); !ok || id <= 0 {
		t.Fatalf("CLIENT ID = %#v", id)
	}
	c.wantErr("unknown subcommand", "CLIENT", "KILL", "1")

	c.want([]any{}, "COMMAND")
	c.want([]any{}, "COMMAND", "DOCS")
	c.want(int64(len(commands)), "COMMAND", "COUNT")

	c.want("OK", "QUIT")
	c.wantClosed()
}

func TestNamespacesAndDurability(t *testing.T) {
	c := start(t).dial()

	c.want("orders", "NAMESPACE", "FOR", "orders:42")
	c.want("sessions", "NAMESPACE", "FOR", "session:abc")
	c.want("default", "NAMESPACE", "FOR", "anything")
	c.wantErr("usage", "NAMESPACE", "DROP", "orders")

	list, ok := c.do("NAMESPACE", "LIST").([]any)
	if !ok || len(list) != 3 {
		t.Fatalf("NAMESPACE LIST = %#v; want three namespaces", list)
	}
	got := map[string]map[string]any{}
	for _, item := range list {
		fields := item.([]any)
		m := map[string]any{}
		for i := 0; i < len(fields); i += 2 {
			m[fields[i].(string)] = fields[i+1]
		}
		got[m["name"].(string)] = m
	}
	if m := got["orders"]; m["profile"] != "strict" || m["prefix"] != "orders:" || m["max_size"] != i64+0 {
		t.Errorf("orders namespace = %v", m)
	}
	if m := got["sessions"]; m["profile"] != "cache" || m["max_size"] != i64+(64<<20) || m["default_ttl_ms"] != i64+3600000 {
		t.Errorf("sessions namespace = %v", m)
	}
	if m := got["default"]; m["profile"] != "cache" || m["prefix"] != "" {
		t.Errorf("default namespace = %v", m)
	}
	// A durable namespace is one column family; a cache namespace is a
	// series of buckets and starts with one.
	if got["orders"]["buckets"] != i64+1 || got["sessions"]["buckets"] != i64+1 {
		t.Errorf("buckets: orders=%v sessions=%v; want 1 and 1", got["orders"]["buckets"], got["sessions"]["buckets"])
	}

	c.want("cache", "DURABILITY")
	c.want("OK", "DURABILITY", "strict")
	c.want("strict", "DURABILITY")
	c.want("OK", "SET", "k", "v") // still works, now fsynced
	c.want("v", "GET", "k")
	c.wantErr("unknown profile", "DURABILITY", "extreme")
	c.want("strict", "DURABILITY")
	c.wantErr("wrong number of arguments", "DURABILITY", "a", "b")

	// The floor belongs to the connection that set it.
	start(t).dial().want("cache", "DURABILITY")
}

func TestInfoAndDBSize(t *testing.T) {
	c := start(t).dial()
	for i := 0; i < 10; i++ {
		c.want("OK", "SET", fmt.Sprintf("orders:%d", i), "v")
	}
	if n, ok := c.do("DBSIZE").(int64); !ok || n != 10 {
		t.Fatalf("DBSIZE = %#v; want 10", n)
	}
	info, ok := c.do("INFO").(string)
	if !ok {
		t.Fatalf("INFO did not return a bulk string")
	}
	for _, want := range []string{
		"levelquorum_version:" + Version,
		"connected_clients:1",
		`namespace_orders:prefix="orders:",profile=strict`,
		`namespace_sessions:prefix="session:",profile=cache,max_size=67108864,default_ttl_ms=3600000`,
		"buckets=1,evicted_buckets=0,promotions=0",
		"total_commands_processed:11", // ten SETs and the DBSIZE; INFO itself is counted after it replies
		"hot_cache_entries:10",
		"hot_cache_capacity_bytes:16777216",
		"db0:keys=10",
	} {
		if !strings.Contains(info, want) {
			t.Errorf("INFO lacks %q:\n%s", want, info)
		}
	}
}

// Shutdown disconnects clients and stops accepting new ones.
func TestShutdown(t *testing.T) {
	h := start(t)
	c := h.dial()
	c.want("OK", "SET", "orders:1", "v")

	h.srv.Shutdown()
	c.wantClosed()
	if nc, err := net.DialTimeout("tcp", h.addr, time.Second); err == nil {
		nc.Close()
		t.Fatal("the listener still accepts connections after Shutdown")
	}
	// Shutdown waited for running commands, so the engine is safe to use
	// directly, and the write is there.
	ent, err := h.eng.Get([]byte("orders:1"))
	if err != nil || ent == nil || string(ent.Value) != "v" {
		t.Fatalf("after Shutdown, Get = %+v, %v", ent, err)
	}
}

// metricValue returns the value of the sample with exactly this name and
// label set, or fails the test.
func metricValue(t *testing.T, text, sample string) float64 {
	t.Helper()
	for _, line := range strings.Split(text, "\n") {
		if rest, ok := strings.CutPrefix(line, sample+" "); ok {
			v, err := strconv.ParseFloat(rest, 64)
			if err != nil {
				t.Fatalf("metric %s has value %q", sample, rest)
			}
			return v
		}
	}
	t.Fatalf("metric %s not found in:\n%s", sample, text)
	return 0
}

func TestMetrics(t *testing.T) {
	h := start(t)
	c := h.dial()
	c.want("OK", "SET", "orders:1", "v")
	for i := 0; i < 5; i++ {
		c.want("v", "GET", "orders:1")
	}
	c.want(nil, "GET", "orders:missing")
	c.wantErr("not an integer", "INCR", "orders:1")
	c.wantErr("wrong number of arguments", "GET")
	c.wantErr("unknown command", "NOSUCH")

	rec := httptest.NewRecorder()
	h.srv.MetricsHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q", ct)
	}
	text := rec.Body.String()

	for sample, want := range map[string]float64{
		`levelquorum_commands_total{command="get"}`:                                  7,
		`levelquorum_command_errors_total{command="get"}`:                            1,
		`levelquorum_commands_total{command="set"}`:                                  1,
		`levelquorum_command_errors_total{command="set"}`:                            0,
		`levelquorum_command_errors_total{command="incr"}`:                           1,
		`levelquorum_commands_total{command="unknown"}`:                              1,
		`levelquorum_command_duration_seconds_count{command="get"}`:                  7,
		`levelquorum_command_duration_seconds_bucket{command="get",le="+Inf"}`:       7,
		`levelquorum_connected_clients`:                                              1,
		`levelquorum_namespace_buckets{namespace="orders",profile="strict"}`:         1,
		`levelquorum_namespace_max_size_bytes{namespace="sessions",profile="cache"}`: 64 << 20,
		`levelquorum_hot_cache_entries`:                                              1,
		`levelquorum_hot_cache_hits_total`:                                           6, // five GETs and the INCR
		`levelquorum_hot_cache_misses_total`:                                         1, // the GET of a missing key
		`levelquorum_hot_cache_capacity_bytes`:                                       16 << 20,
	} {
		if got := metricValue(t, text, sample); got != want {
			t.Errorf("%s = %v; want %v", sample, got, want)
		}
	}
	if sum := metricValue(t, text, `levelquorum_command_duration_seconds_sum{command="get"}`); sum <= 0 || sum > 5 {
		t.Errorf("total GET time = %v seconds; want a small positive number", sum)
	}
	// Histogram buckets are cumulative.
	prev := 0.0
	for _, line := range strings.Split(text, "\n") {
		if rest, ok := strings.CutPrefix(line, `levelquorum_command_duration_seconds_bucket{command="get",le=`); ok {
			v, _ := strconv.ParseFloat(rest[strings.LastIndex(rest, " ")+1:], 64)
			if v < prev {
				t.Fatalf("histogram bucket decreases: %s", line)
			}
			prev = v
		}
	}
	// Commands that were never called are not listed.
	if strings.Contains(text, `command="mset"`) {
		t.Error("metrics list a command that was never called")
	}
	// Every sample line must parse as "name[{labels}] number".
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.LastIndex(line, " ")
		if _, err := strconv.ParseFloat(line[i+1:], 64); i < 0 || err != nil || !strings.HasPrefix(line, "levelquorum_") {
			t.Fatalf("malformed metrics line %q", line)
		}
	}
}
