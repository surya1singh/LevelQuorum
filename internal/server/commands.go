package server

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/surya1singh/LevelQuorum/internal/storage"
)

// command describes one Redis command. arity counts the command name: a
// positive value is the exact number of arguments, a negative one a minimum.
type command struct {
	arity int
	fn    func(c *client, args [][]byte)
}

var commands map[string]command

func init() {
	commands = map[string]command{
		// Connection
		"PING":    {-1, cmdPing},
		"ECHO":    {2, cmdEcho},
		"QUIT":    {-1, cmdQuit},
		"SELECT":  {2, cmdSelect},
		"AUTH":    {-2, cmdAuth},
		"HELLO":   {-1, cmdHello},
		"CLIENT":  {-2, cmdClient},
		"COMMAND": {-1, cmdCommand},

		// Strings
		"GET":    {2, cmdGet},
		"SET":    {-3, cmdSet},
		"SETNX":  {3, cmdSetNX},
		"SETEX":  {4, cmdSetEX},
		"PSETEX": {4, cmdSetEX},
		"GETSET": {3, cmdGetSet},
		"GETDEL": {2, cmdGetDel},
		"MGET":   {-2, cmdMGet},
		"MSET":   {-3, cmdMSet},
		"APPEND": {3, cmdAppend},
		"STRLEN": {2, cmdStrLen},
		"INCR":   {2, cmdIncr},
		"DECR":   {2, cmdIncr},
		"INCRBY": {3, cmdIncr},
		"DECRBY": {3, cmdIncr},

		// Keys
		"DEL":         {-2, cmdDel},
		"UNLINK":      {-2, cmdDel},
		"EXISTS":      {-2, cmdExists},
		"TYPE":        {2, cmdType},
		"EXPIRE":      {-3, cmdExpire},
		"PEXPIRE":     {-3, cmdExpire},
		"EXPIREAT":    {-3, cmdExpire},
		"PEXPIREAT":   {-3, cmdExpire},
		"PERSIST":     {2, cmdPersist},
		"TTL":         {2, cmdTTL},
		"PTTL":        {2, cmdTTL},
		"EXPIRETIME":  {2, cmdTTL},
		"PEXPIRETIME": {2, cmdTTL},

		// Server
		"DBSIZE": {1, cmdDBSize},
		"INFO":   {-1, cmdInfo},

		// LevelQuorum extensions
		"NAMESPACE":  {-2, cmdNamespace},
		"DURABILITY": {-1, cmdDurability},
	}
}

func upper(b []byte) string { return strings.ToUpper(string(b)) }

func parseInt(b []byte) (int64, bool) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	return n, err == nil
}

const errNotInt = "ERR value is not an integer or out of range"

// deadline returns base + n*unit in milliseconds, or false on overflow. unit
// is 1000 for seconds and 1 for milliseconds; base is the current time for a
// relative expiry and 0 for an absolute one.
func deadline(n, unit, base int64) (int64, bool) {
	if n > math.MaxInt64/unit || n < math.MinInt64/unit {
		return 0, false
	}
	n *= unit
	if n > 0 && base > math.MaxInt64-n {
		return 0, false
	}
	return base + n, true
}

// ---- Connection ----

func cmdPing(c *client, args [][]byte) {
	switch len(args) {
	case 1:
		c.w.SimpleString("PONG")
	case 2:
		c.w.Bulk(args[1])
	default:
		c.wrongArgs("PING")
	}
}

func cmdEcho(c *client, args [][]byte) { c.w.Bulk(args[1]) }

func cmdQuit(c *client, _ [][]byte) {
	c.w.SimpleString("OK")
	c.quit = true
}

func cmdSelect(c *client, args [][]byte) {
	n, ok := parseInt(args[1])
	switch {
	case !ok:
		c.w.Error("ERR invalid DB index")
	case n != 0:
		c.w.Error("ERR DB index is out of range; use namespaces (key prefixes) instead of numbered databases")
	default:
		c.w.SimpleString("OK")
	}
}

func cmdAuth(c *client, _ [][]byte) {
	c.w.Error("ERR Client sent AUTH, but no password is set")
}

func cmdHello(c *client, args [][]byte) {
	if len(args) > 1 {
		if v, ok := parseInt(args[1]); !ok || v != 2 {
			c.w.Error("NOPROTO unsupported protocol version; only RESP2 is supported")
			return
		}
	}
	for i := 2; i < len(args); i++ {
		switch upper(args[i]) {
		case "SETNAME":
			if i+1 >= len(args) {
				c.w.Error("ERR Syntax error in HELLO option 'setname'")
				return
			}
			c.name = string(args[i+1])
			i++
		case "AUTH":
			cmdAuth(c, nil)
			return
		default:
			c.w.Error(fmt.Sprintf("ERR Syntax error in HELLO option '%.64s'", args[i]))
			return
		}
	}
	c.w.ArrayLen(14)
	c.w.BulkString("server")
	c.w.BulkString("levelquorum")
	c.w.BulkString("version")
	c.w.BulkString(Version)
	c.w.BulkString("proto")
	c.w.Int(2)
	c.w.BulkString("id")
	c.w.Int(c.id)
	c.w.BulkString("mode")
	c.w.BulkString("standalone")
	c.w.BulkString("role")
	c.w.BulkString("master")
	c.w.BulkString("modules")
	c.w.ArrayLen(0)
}

func cmdClient(c *client, args [][]byte) {
	switch sub := upper(args[1]); {
	case sub == "SETNAME" && len(args) == 3:
		c.name = string(args[2])
		c.w.SimpleString("OK")
	case sub == "GETNAME" && len(args) == 2:
		if c.name == "" {
			c.w.Null()
		} else {
			c.w.BulkString(c.name)
		}
	case sub == "ID" && len(args) == 2:
		c.w.Int(c.id)
	case sub == "SETINFO" && len(args) == 4:
		// Client libraries announce their name and version; nothing to do.
		c.w.SimpleString("OK")
	default:
		c.w.Error(fmt.Sprintf("ERR unknown subcommand or wrong number of arguments for 'client|%.64s'", strings.ToLower(string(args[1]))))
	}
}

// cmdCommand answers the introspection that clients such as redis-cli send
// on connect. They accept an empty description.
func cmdCommand(c *client, args [][]byte) {
	if len(args) == 2 && upper(args[1]) == "COUNT" {
		c.w.Int(int64(len(commands)))
		return
	}
	c.w.ArrayLen(0)
}

// ---- Strings ----

func cmdGet(c *client, args [][]byte) {
	ent, err := c.srv.eng.Get(args[1])
	if err != nil {
		c.fail(err)
		return
	}
	c.bulkOrNull(ent)
}

func (c *client) bulkOrNull(ent *storage.Entry) {
	if ent == nil {
		c.w.Null()
	} else {
		c.w.Bulk(ent.Value)
	}
}

func cmdSet(c *client, args [][]byte) {
	var o storage.SetOptions
	expirySet := false
	now := c.srv.eng.NowMillis()

	for i := 3; i < len(args); i++ {
		opt := upper(args[i])
		switch opt {
		case "NX":
			o.NX = true
		case "XX":
			o.XX = true
		case "GET":
			o.ReturnOld = true
		case "KEEPTTL":
			o.KeepTTL = true
		case "EX", "PX", "EXAT", "PXAT":
			if expirySet || i+1 >= len(args) {
				c.w.Error("ERR syntax error")
				return
			}
			n, ok := parseInt(args[i+1])
			if !ok {
				c.w.Error(errNotInt)
				return
			}
			unit, base := int64(1), int64(0)
			if opt == "EX" || opt == "EXAT" {
				unit = 1000
			}
			if opt == "EX" || opt == "PX" {
				base = now
			}
			at, fits := deadline(n, unit, base)
			if n <= 0 || !fits {
				c.w.Error("ERR invalid expire time in 'set' command")
				return
			}
			o.ExpireAt = at
			expirySet = true
			i++
		default:
			c.w.Error("ERR syntax error")
			return
		}
	}
	if (o.NX && o.XX) || (o.KeepTTL && expirySet) {
		c.w.Error("ERR syntax error")
		return
	}

	res, err := c.srv.eng.Set(args[1], args[2], o, c.floor)
	switch {
	case err != nil:
		c.fail(err)
	case o.ReturnOld:
		c.bulkOrNull(res.Old)
	case res.Written:
		c.w.SimpleString("OK")
	default:
		c.w.Null()
	}
}

func cmdSetNX(c *client, args [][]byte) {
	res, err := c.srv.eng.Set(args[1], args[2], storage.SetOptions{NX: true}, c.floor)
	if err != nil {
		c.fail(err)
		return
	}
	c.w.Int(boolInt(res.Written))
}

// cmdSetEX implements SETEX and PSETEX: key, ttl, value.
func cmdSetEX(c *client, args [][]byte) {
	name := upper(args[0])
	n, ok := parseInt(args[2])
	if !ok {
		c.w.Error(errNotInt)
		return
	}
	unit := int64(1000)
	if name == "PSETEX" {
		unit = 1
	}
	at, fits := deadline(n, unit, c.srv.eng.NowMillis())
	if n <= 0 || !fits {
		c.w.Error("ERR invalid expire time in '" + strings.ToLower(name) + "' command")
		return
	}
	if _, err := c.srv.eng.Set(args[1], args[3], storage.SetOptions{ExpireAt: at}, c.floor); err != nil {
		c.fail(err)
		return
	}
	c.w.SimpleString("OK")
}

func cmdGetSet(c *client, args [][]byte) {
	res, err := c.srv.eng.Set(args[1], args[2], storage.SetOptions{ReturnOld: true}, c.floor)
	if err != nil {
		c.fail(err)
		return
	}
	c.bulkOrNull(res.Old)
}

func cmdGetDel(c *client, args [][]byte) {
	ent, err := c.srv.eng.GetDel(args[1], c.floor)
	if err != nil {
		c.fail(err)
		return
	}
	c.bulkOrNull(ent)
}

func cmdMGet(c *client, args [][]byte) {
	entries := make([]*storage.Entry, len(args)-1)
	for i, k := range args[1:] {
		ent, err := c.srv.eng.Get(k)
		if err != nil {
			c.fail(err)
			return
		}
		entries[i] = ent
	}
	c.w.ArrayLen(len(entries))
	for _, ent := range entries {
		c.bulkOrNull(ent)
	}
}

func cmdMSet(c *client, args [][]byte) {
	if len(args)%2 != 1 {
		c.wrongArgs("MSET")
		return
	}
	if err := c.srv.eng.MSet(args[1:], c.floor); err != nil {
		c.fail(err)
		return
	}
	c.w.SimpleString("OK")
}

func cmdAppend(c *client, args [][]byte) {
	n, err := c.srv.eng.Append(args[1], args[2], c.floor)
	if err != nil {
		c.fail(err)
		return
	}
	c.w.Int(int64(n))
}

func cmdStrLen(c *client, args [][]byte) {
	ent, err := c.srv.eng.Get(args[1])
	if err != nil {
		c.fail(err)
		return
	}
	if ent == nil {
		c.w.Int(0)
		return
	}
	c.w.Int(int64(len(ent.Value)))
}

// cmdIncr implements INCR, DECR, INCRBY and DECRBY.
func cmdIncr(c *client, args [][]byte) {
	name := upper(args[0])
	delta := int64(1)
	if len(args) == 3 {
		var ok bool
		if delta, ok = parseInt(args[2]); !ok {
			c.w.Error(errNotInt)
			return
		}
	}
	if name == "DECR" || name == "DECRBY" {
		if delta == math.MinInt64 {
			c.w.Error("ERR decrement would overflow")
			return
		}
		delta = -delta
	}
	n, err := c.srv.eng.IncrBy(args[1], delta, c.floor)
	if err != nil {
		c.fail(err)
		return
	}
	c.w.Int(n)
}

// ---- Keys ----

func cmdDel(c *client, args [][]byte) {
	n, err := c.srv.eng.Delete(args[1:], c.floor)
	if err != nil {
		c.fail(err)
		return
	}
	c.w.Int(int64(n))
}

func cmdExists(c *client, args [][]byte) {
	var n int64
	for _, k := range args[1:] {
		_, ok, err := c.srv.eng.Meta(k)
		if err != nil {
			c.fail(err)
			return
		}
		n += boolInt(ok)
	}
	c.w.Int(n)
}

func cmdType(c *client, args [][]byte) {
	_, ok, err := c.srv.eng.Meta(args[1])
	switch {
	case err != nil:
		c.fail(err)
	case ok:
		c.w.SimpleString("string")
	default:
		c.w.SimpleString("none")
	}
}

// cmdExpire implements EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT.
func cmdExpire(c *client, args [][]byte) {
	name := upper(args[0])
	if len(args) > 3 {
		c.w.Error("ERR the NX, XX, GT and LT options of " + name + " are not supported")
		return
	}
	n, ok := parseInt(args[2])
	if !ok {
		c.w.Error(errNotInt)
		return
	}
	unit, base := int64(1), int64(0)
	if name == "EXPIRE" || name == "EXPIREAT" {
		unit = 1000
	}
	if name == "EXPIRE" || name == "PEXPIRE" {
		base = c.srv.eng.NowMillis()
	}
	at, fits := deadline(n, unit, base)
	if !fits {
		c.w.Error("ERR invalid expire time in '" + strings.ToLower(name) + "' command")
		return
	}
	found, err := c.srv.eng.Expire(args[1], at, c.floor)
	if err != nil {
		c.fail(err)
		return
	}
	c.w.Int(boolInt(found))
}

func cmdPersist(c *client, args [][]byte) {
	changed, err := c.srv.eng.Persist(args[1], c.floor)
	if err != nil {
		c.fail(err)
		return
	}
	c.w.Int(boolInt(changed))
}

// cmdTTL implements TTL, PTTL, EXPIRETIME and PEXPIRETIME. All four answer
// -2 for a missing key and -1 for a key that never expires.
func cmdTTL(c *client, args [][]byte) {
	exp, ok, err := c.srv.eng.Meta(args[1])
	switch {
	case err != nil:
		c.fail(err)
		return
	case !ok:
		c.w.Int(-2)
		return
	case exp == 0:
		c.w.Int(-1)
		return
	}
	remaining := exp - c.srv.eng.NowMillis()
	switch upper(args[0]) {
	case "TTL":
		c.w.Int((remaining + 500) / 1000)
	case "PTTL":
		c.w.Int(remaining)
	case "EXPIRETIME":
		c.w.Int(exp / 1000)
	default:
		c.w.Int(exp)
	}
}

// ---- Server ----

// cmdDBSize answers with RocksDB's estimate of the number of keys. Unlike in
// Redis it is not exact: it can include keys that were overwritten, deleted
// or have expired but are not compacted away yet.
func cmdDBSize(c *client, _ [][]byte) {
	var n uint64
	for _, st := range c.srv.eng.Stats() {
		n += st.EstimatedKeys
	}
	c.w.Int(int64(n))
}

func cmdInfo(c *client, _ [][]byte) {
	s := c.srv
	var b strings.Builder
	var keys uint64

	fmt.Fprintf(&b, "# Server\r\n")
	fmt.Fprintf(&b, "levelquorum_version:%s\r\n", Version)
	// Some client libraries read redis_version to decide which commands to
	// use; report the version whose string commands are implemented here.
	fmt.Fprintf(&b, "redis_version:7.0.0\r\n")
	fmt.Fprintf(&b, "redis_mode:standalone\r\n")
	fmt.Fprintf(&b, "uptime_in_seconds:%d\r\n", int64(time.Since(s.started).Seconds()))
	fmt.Fprintf(&b, "\r\n# Clients\r\n")
	fmt.Fprintf(&b, "connected_clients:%d\r\n", s.clientCount())
	fmt.Fprintf(&b, "\r\n# Stats\r\n")
	fmt.Fprintf(&b, "total_commands_processed:%d\r\n", s.totalCommands())
	fmt.Fprintf(&b, "\r\n# Memory\r\n")
	fmt.Fprintf(&b, "block_cache_bytes:%d\r\n", s.eng.BlockCacheUsage())
	if hot, ok := s.eng.HotStats(); ok {
		fmt.Fprintf(&b, "hot_cache_bytes:%d\r\n", hot.Bytes)
		fmt.Fprintf(&b, "hot_cache_capacity_bytes:%d\r\n", hot.Capacity)
		fmt.Fprintf(&b, "hot_cache_entries:%d\r\n", hot.Entries)
		fmt.Fprintf(&b, "hot_cache_hits:%d\r\n", hot.Hits)
		fmt.Fprintf(&b, "hot_cache_misses:%d\r\n", hot.Misses)
	} else {
		fmt.Fprintf(&b, "hot_cache_capacity_bytes:0\r\n")
	}
	fmt.Fprintf(&b, "\r\n# Namespaces\r\n")
	for _, st := range s.eng.Stats() {
		keys += st.EstimatedKeys
		fmt.Fprintf(&b, "namespace_%s:prefix=%q,profile=%s,max_size=%d,default_ttl_ms=%d,estimated_keys=%d,disk_bytes=%d,memtable_bytes=%d,buckets=%d,evicted_buckets=%d,promotions=%d\r\n",
			st.Spec.Name, st.Spec.Prefix, st.Spec.Profile, st.Spec.MaxSize, st.Spec.DefaultTTL.Milliseconds(),
			st.EstimatedKeys, st.DiskBytes, st.MemtableBytes, st.Buckets, st.EvictedBuckets, st.Promotions)
	}
	fmt.Fprintf(&b, "\r\n# Keyspace\r\n")
	fmt.Fprintf(&b, "db0:keys=%d,expires=0,avg_ttl=0\r\n", keys)
	c.w.BulkString(b.String())
}

// ---- LevelQuorum extensions ----

// cmdNamespace implements NAMESPACE LIST and NAMESPACE FOR <key>.
func cmdNamespace(c *client, args [][]byte) {
	switch sub := upper(args[1]); {
	case sub == "LIST" && len(args) == 2:
		stats := c.srv.eng.Stats()
		c.w.ArrayLen(len(stats))
		for _, st := range stats {
			c.w.ArrayLen(16)
			c.w.BulkString("name")
			c.w.BulkString(st.Spec.Name)
			c.w.BulkString("prefix")
			c.w.BulkString(st.Spec.Prefix)
			c.w.BulkString("profile")
			c.w.BulkString(st.Spec.Profile.String())
			c.w.BulkString("max_size")
			c.w.Int(int64(st.Spec.MaxSize))
			c.w.BulkString("default_ttl_ms")
			c.w.Int(st.Spec.DefaultTTL.Milliseconds())
			c.w.BulkString("estimated_keys")
			c.w.Int(int64(st.EstimatedKeys))
			c.w.BulkString("disk_bytes")
			c.w.Int(int64(st.DiskBytes))
			c.w.BulkString("buckets")
			c.w.Int(int64(st.Buckets))
		}
	case sub == "FOR" && len(args) == 3:
		c.w.BulkString(c.srv.eng.NamespaceOf(args[2]).Name)
	default:
		c.w.Error("ERR usage: NAMESPACE LIST | NAMESPACE FOR <key>")
	}
}

// cmdDurability reads or sets this connection's durability floor. Each write
// uses the higher of the floor and its namespace's profile, so a connection
// can make its writes safer than the namespace default but never weaker.
// The floor does not affect eviction, which is a property of the namespace.
func cmdDurability(c *client, args [][]byte) {
	switch len(args) {
	case 1:
		c.w.BulkString(c.floor.String())
	case 2:
		p, err := storage.ParseProfile(string(args[1]))
		if err != nil {
			c.w.Error("ERR " + err.Error())
			return
		}
		c.floor = p
		c.w.SimpleString("OK")
	default:
		c.wrongArgs("DURABILITY")
	}
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
