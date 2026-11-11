// Package server exposes a storage engine over the Redis protocol.
package server

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/surya1singh/LevelQuorum/internal/resp"
	"github.com/surya1singh/LevelQuorum/internal/storage"
)

// Version is reported by INFO and HELLO.
const Version = "0.2.0"

// DefaultMaxBulkSize is the largest key or value accepted when Options does
// not set one.
const DefaultMaxBulkSize = 64 << 20

// Options configures a Server.
type Options struct {
	// MaxBulkSize is the largest key or value, in bytes, a client may send.
	MaxBulkSize int
	// Logger receives connection-level errors. Default slog.Default().
	Logger *slog.Logger
}

// Server serves one storage engine to any number of client connections.
type Server struct {
	eng     *storage.Engine
	log     *slog.Logger
	maxBulk int
	started time.Time
	nextID  atomic.Int64
	// stats has one fixed entry per command, so it is read without locking.
	stats map[string]*commandStats

	mu     sync.Mutex
	ln     net.Listener
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup
}

// New returns a Server for eng. The caller keeps ownership of eng and closes
// it after Shutdown.
func New(eng *storage.Engine, o Options) *Server {
	if o.MaxBulkSize <= 0 {
		o.MaxBulkSize = DefaultMaxBulkSize
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return &Server{
		eng:     eng,
		log:     o.Logger,
		maxBulk: o.MaxBulkSize,
		started: time.Now(),
		stats:   newStatsTable(),
		conns:   make(map[net.Conn]struct{}),
	}
}

// Serve accepts connections on ln until Shutdown is called, then returns
// nil. Any other return value is the error that stopped it.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		ln.Close()
		return nil
	}
	s.ln = ln
	s.mu.Unlock()

	for {
		nc, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			// Typically "too many open files": keep serving the clients
			// that are connected and try again shortly.
			s.log.Error("accept failed", "err", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			nc.Close()
			return nil
		}
		s.conns[nc] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.handle(nc)
	}
}

// Shutdown stops accepting connections, closes the open ones, and waits
// until no command is running. After it returns the engine can be closed.
func (s *Server) Shutdown() {
	s.mu.Lock()
	s.closed = true
	if s.ln != nil {
		s.ln.Close()
	}
	for nc := range s.conns {
		nc.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *Server) clientCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// client is the state of one connection.
type client struct {
	srv  *Server
	id   int64
	name string
	// floor is the durability this connection asked for with DURABILITY.
	// Each write uses the higher of floor and its namespace's profile.
	floor storage.Profile
	r     *resp.Reader
	w     *resp.Writer
	quit  bool
}

func (s *Server) handle(nc net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.conns, nc)
		s.mu.Unlock()
		nc.Close()
	}()
	defer func() {
		if p := recover(); p != nil {
			s.log.Error("panic while serving a client", "remote", nc.RemoteAddr().String(), "panic", p, "stack", string(debug.Stack()))
		}
	}()

	c := &client{
		srv: s,
		id:  s.nextID.Add(1),
		r:   resp.NewReader(nc, s.maxBulk),
		w:   resp.NewWriter(nc),
	}
	for {
		args, err := c.r.ReadCommand()
		if err != nil {
			var perr *resp.ProtocolError
			if errors.As(err, &perr) {
				c.w.Error("ERR " + perr.Error())
				c.w.Flush()
			}
			return
		}
		if len(args) > 0 {
			s.dispatch(c, args)
		}
		// Reply once the client has nothing more queued. A pipelined batch
		// is therefore answered with a single write.
		if c.quit || c.r.Buffered() == 0 {
			if err := c.w.Flush(); err != nil {
				return
			}
		}
		if c.quit {
			return
		}
	}
}

func (s *Server) dispatch(c *client, args [][]byte) {
	name := strings.ToUpper(string(args[0]))
	cmd, ok := commands[name]
	if !ok {
		var b strings.Builder
		for _, a := range args[1:] {
			if b.Len() > 128 {
				break
			}
			fmt.Fprintf(&b, "'%.64s' ", a)
		}
		c.w.Error(fmt.Sprintf("ERR unknown command '%.64s', with args beginning with: %s", args[0], b.String()))
		s.stats[unknownCommand].observe(0, true)
		return
	}
	errorsBefore := c.w.ErrorCount()
	start := time.Now()
	if (cmd.arity > 0 && len(args) != cmd.arity) || (cmd.arity < 0 && len(args) < -cmd.arity) {
		c.wrongArgs(name)
	} else {
		cmd.fn(c, args)
	}
	s.stats[name].observe(time.Since(start), c.w.ErrorCount() != errorsBefore)
}

func (c *client) wrongArgs(name string) {
	c.w.Error("ERR wrong number of arguments for '" + strings.ToLower(name) + "' command")
}

// fail reports an engine error to the client.
func (c *client) fail(err error) {
	switch {
	case errors.Is(err, storage.ErrNotInteger), errors.Is(err, storage.ErrOverflow):
		c.w.Error("ERR " + err.Error())
	default:
		c.srv.log.Error("storage error", "err", err)
		c.w.Error("ERR internal error: " + err.Error())
	}
}
