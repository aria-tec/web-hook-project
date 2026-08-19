package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"

	"web-hook-project/internal/api"
	"web-hook-project/internal/dispatcher"
	"web-hook-project/internal/idempotency"
	"web-hook-project/internal/outbox"
	"web-hook-project/internal/queue"
	"web-hook-project/internal/retry"
	"web-hook-project/internal/storage"
	"web-hook-project/internal/telemetry"
	"web-hook-project/internal/worker"
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if valStr := os.Getenv(key); valStr != "" {
		if val, err := strconv.Atoi(valStr); err == nil {
			return val
		}
	}
	return defaultVal
}

func main() {
	port := getEnv("PORT", "8080")
	databaseURL := os.Getenv("DATABASE_URL")
	redisURL := os.Getenv("REDIS_URL")
	workerCount := getEnvInt("WORKER_COUNT", 10)
	relayBatchSize := getEnvInt("RELAY_BATCH_SIZE", 100)

	log.Printf("Starting Webhook Reliability Engine on port :%s (workers: %d, relay_batch: %d)", port, workerCount, relayBatchSize)

	// 1. Telemetry & Metrics
	metrics := telemetry.NewMetrics()

	// 2. Storage Repository (Postgres with fallback to In-Memory)
	var repo storage.Repository
	var db *sql.DB
	if databaseURL != "" {
		var err error
		db, err = sql.Open("pgx", databaseURL)
		if err != nil {
			log.Printf("[WARN] Failed to open PostgreSQL connection: %v. Falling back to in-memory repository.", err)
			repo = storage.NewMemoryRepository()
		} else {
			pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
			if err := db.PingContext(pingCtx); err != nil {
				log.Printf("[WARN] PostgreSQL ping failed: %v. Falling back to in-memory repository.", err)
				repo = storage.NewMemoryRepository()
			} else {
				log.Printf("[INFO] Connected to PostgreSQL storage at %s", databaseURL)
				repo = storage.NewPostgresRepository(db)
			}
			pingCancel()
		}
	} else {
		log.Printf("[INFO] No DATABASE_URL specified. Using in-memory storage repository.")
		repo = storage.NewMemoryRepository()
	}

	// 3. Redis Streams Queue & Idempotency Guard (Redis with fallback to In-Memory)
	var streamQueue queue.StreamQueue
	var guard idempotency.Guard
	var redisClient *redis.Client
	if redisURL != "" {
		opt, err := redis.ParseURL(redisURL)
		if err != nil {
			opt = &redis.Options{Addr: redisURL}
		}
		redisClient = redis.NewClient(opt)
		pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := redisClient.Ping(pingCtx).Err(); err != nil {
			log.Printf("[WARN] Redis ping failed: %v. Falling back to in-memory stream queue and guard.", err)
			streamQueue = queue.NewMemoryStreamQueue()
			guard = idempotency.NewMemoryGuard()
		} else {
			log.Printf("[INFO] Connected to Redis at %s", redisURL)
			streamQueue = queue.NewRedisStreamQueue(redisClient)
			guard = idempotency.NewRedisGuard(redisClient)
		}
		pingCancel()
	} else {
		log.Printf("[INFO] No REDIS_URL specified. Using in-memory stream queue and idempotency guard.")
		streamQueue = queue.NewMemoryStreamQueue()
		guard = idempotency.NewMemoryGuard()
	}

	// 4. Safe HTTP Dispatcher
	safeClient := dispatcher.NewSafeHTTPClient(10 * time.Second)
	backoffPolicy := retry.DefaultBackoffPolicy()
	disp := dispatcher.NewDispatcher(safeClient, repo, backoffPolicy).WithMetrics(metrics)

	// 5. Bounded Worker Pool
	workerCfg := worker.Config{
		NumWorkers:   workerCount,
		StreamName:   worker.DefaultStreamName,
		GroupName:    worker.DefaultGroupName,
		BatchSize:    10,
		PollInterval: 100 * time.Millisecond,
	}
	workerPool := worker.NewWorkerPool(workerCfg, streamQueue, repo, disp)

	// 6. Outbox Relay
	relay := outbox.NewRelay(repo, streamQueue)

	// 7. HTTP API Router & Handlers
	apiHandler := api.NewHandler(repo, guard).WithMetrics(metrics)
	router := api.NewRouter(apiHandler, metrics)

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// 8. Background lifecycle management
	rootCtx, rootCancel := context.WithCancel(context.Background())

	if err := workerPool.Start(rootCtx); err != nil {
		log.Fatalf("Failed to start worker pool: %v", err)
	}
	log.Printf("[INFO] Worker pool started with %d workers", workerCount)

	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		log.Printf("[INFO] Outbox relay loop started")
		_ = relay.Start(rootCtx, 100*time.Millisecond, relayBatchSize)
	}()

	go func() {
		log.Printf("[INFO] Server listening on http://0.0.0.0:%s", port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server listen error: %v", err)
		}
	}()

	// 9. Graceful Shutdown on SIGINT/SIGTERM
	stopSig := make(chan os.Signal, 1)
	signal.Notify(stopSig, syscall.SIGINT, syscall.SIGTERM)
	sig := <-stopSig
	log.Printf("[INFO] Received signal %v. Initiating graceful shutdown...", sig)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[WARN] HTTP server shutdown error: %v", err)
	}

	rootCancel()
	<-relayDone
	workerPool.Stop()

	if db != nil {
		_ = db.Close()
	}
	if redisClient != nil {
		_ = redisClient.Close()
	}

	log.Printf("[INFO] Server shutdown cleanly completed.")
}
