// Command pulsed runs the telemetry ingest service.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/Ohimoiza1205/pulse/internal/httpapi"
	"github.com/Ohimoiza1205/pulse/internal/ingest"
	"github.com/Ohimoiza1205/pulse/internal/stream"
	"github.com/Ohimoiza1205/pulse/internal/telemetry"
)

func main() {
	var (
		addr      = flag.String("addr", ":8080", "listen address")
		queueSize = flag.Int("queue", 8192, "bounded ingest queue size")
		workers   = flag.Int("workers", runtime.NumCPU()*2, "ingest worker count")
		retain    = flag.Int("retain", 720, "readings retained per device")
		drain     = flag.Duration("drain", 15*time.Second, "max time to drain in flight work on shutdown")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	store := telemetry.NewStore(*retain)
	hub := stream.NewHub()
	pipe := ingest.New(store, hub, *queueSize, *workers)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pipe.Start(ctx)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           httpapi.New(store, pipe, hub, log).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		// No WriteTimeout: it would guillotine long lived WebSocket streams.
		// Per connection write deadlines in the stream handler cover that path.
		IdleTimeout: 60 * time.Second,
	}

	go func() {
		log.Info("pulse listening",
			"addr", *addr, "workers", *workers, "queue", *queueSize, "retain_per_device", *retain)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutdown signal received, draining")

	// Order matters. Stop taking new requests, then drain queued readings,
	// then hang up on stream subscribers. Reversing this drops in flight work.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), *drain)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown incomplete", "err", err)
	}
	if err := pipe.Shutdown(shutdownCtx); err != nil {
		log.Warn("ingest drain incomplete", "err", err)
	}
	hub.CloseAll()

	accepted, shed, processed := pipe.Stats()
	log.Info("stopped", "accepted", accepted, "shed", shed, "processed", processed)
}
