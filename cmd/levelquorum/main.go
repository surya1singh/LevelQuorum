// Command levelquorum runs a LevelQuorum node: a Redis-compatible key-value
// server that stores its data in RocksDB.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/surya1singh/LevelQuorum/internal/config"
	"github.com/surya1singh/LevelQuorum/internal/server"
	"github.com/surya1singh/LevelQuorum/internal/storage"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "levelquorum:", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to the YAML configuration file (optional)")
	listen := flag.String("listen", "", "address to listen on; overrides the configuration file")
	dataDir := flag.String("data-dir", "", "data directory; overrides the configuration file")
	flag.Parse()
	if flag.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", flag.Arg(0))
	}

	cfg := config.Default()
	if *configPath != "" {
		var err error
		if cfg, err = config.Load(*configPath); err != nil {
			return err
		}
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	specs, err := cfg.NamespaceSpecs()
	if err != nil {
		return err
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	eng, err := storage.Open(storage.Options{
		Dir:             cfg.DataDir,
		Namespaces:      specs,
		BlockCacheSize:  uint64(cfg.BlockCacheSize),
		WriteBufferSize: uint64(cfg.WriteBufferSize),
		Logger:          log,
	})
	if err != nil {
		return err
	}
	for _, st := range eng.Stats() {
		log.Info("namespace", "name", st.Spec.Name, "prefix", st.Spec.Prefix, "profile", st.Spec.Profile.String(),
			"max_size", st.Spec.MaxSize, "default_ttl", st.Spec.DefaultTTL.String(), "disk_bytes", st.DiskBytes)
	}

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		eng.Close()
		return err
	}
	srv := server.New(eng, server.Options{MaxBulkSize: int(cfg.MaxBulkSize), Logger: log})
	log.Info("listening", "addr", ln.Addr().String(), "data_dir", cfg.DataDir, "version", server.Version)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()

	var serveErr error
	select {
	case sig := <-stop:
		log.Info("shutting down", "signal", sig.String())
	case serveErr = <-served:
	}
	srv.Shutdown()
	if err := eng.Close(); err != nil {
		return fmt.Errorf("closing the database: %w", err)
	}
	return serveErr
}
