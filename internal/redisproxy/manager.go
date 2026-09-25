package redisproxy

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrUnauthorized = errors.New("unauthorized token")
var ErrClosed = errors.New("pool manager is closed")

type Stats struct {
	Active  int
	Created uint64
	Evicted uint64
}

type Backend struct {
	Token            string
	ID               string
	ConnectionString string
	MaxConnections   int
}

type poolKey struct {
	tokenHash        [sha256.Size]byte
	connectionString string
}

type backend struct {
	id             string
	key            poolKey
	options        redis.Options
	maxConnections int
}

type managedPool struct {
	backend  *backend
	client   *redis.Client
	inUse    int
	lastUsed time.Time
}

type Manager struct {
	mu          sync.Mutex
	backends    map[[sha256.Size]byte]*backend
	pools       map[poolKey]*managedPool
	idleTimeout time.Duration
	logger      *slog.Logger
	stop        chan struct{}
	done        chan struct{}
	closed      bool
	leases      sync.WaitGroup
	created     atomic.Uint64
	evicted     atomic.Uint64
}

func NewManager(backends []Backend, idleTimeout time.Duration, logger *slog.Logger) (*Manager, error) {
	if len(backends) == 0 {
		return nil, fmt.Errorf("at least one Redis backend is required")
	}

	if idleTimeout <= 0 {
		return nil, fmt.Errorf("idle timeout must be positive")
	}

	if logger == nil {
		logger = slog.Default()
	}

	manager := &Manager{
		backends:    make(map[[sha256.Size]byte]*backend, len(backends)),
		pools:       make(map[poolKey]*managedPool),
		idleTimeout: idleTimeout,
		logger:      logger,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}

	for _, configured := range backends {
		if configured.Token == "" {
			return nil, fmt.Errorf("backend %q has an empty token", configured.ID)
		}

		if configured.ID == "" {
			return nil, fmt.Errorf("backend ID is required")
		}

		if configured.MaxConnections < 1 {
			return nil, fmt.Errorf("backend %q max connections must be positive", configured.ID)
		}

		options, err := redis.ParseURL(configured.ConnectionString)

		if err != nil {
			return nil, fmt.Errorf("backend %q has an invalid Redis connection string", configured.ID)
		}

		options.Protocol = 2
		options.PoolSize = configured.MaxConnections
		options.MaxActiveConns = configured.MaxConnections
		options.MaxRetries = -1

		tokenHash := sha256.Sum256([]byte(configured.Token))

		if _, exists := manager.backends[tokenHash]; exists {
			return nil, fmt.Errorf("duplicate token for backend %q", configured.ID)
		}

		poolBackend := &backend{
			id: configured.ID,
			key: poolKey{
				tokenHash:        tokenHash,
				connectionString: configured.ConnectionString,
			},
			options:        *options,
			maxConnections: configured.MaxConnections,
		}
		manager.backends[tokenHash] = poolBackend
	}

	go manager.evictIdlePools()

	return manager, nil
}

func (m *Manager) Acquire(token string) (*redis.Client, func(), error) {
	tokenHash := sha256.Sum256([]byte(token))
	poolBackend, authorized := m.backends[tokenHash]

	if !authorized {
		return nil, nil, ErrUnauthorized
	}

	m.mu.Lock()

	if m.closed {
		m.mu.Unlock()
		return nil, nil, ErrClosed
	}

	pool := m.pools[poolBackend.key]

	if pool == nil {
		options := poolBackend.options
		pool = &managedPool{
			backend:  poolBackend,
			client:   redis.NewClient(&options),
			lastUsed: time.Now(),
		}
		m.pools[poolBackend.key] = pool
		m.created.Add(1)
		m.logger.Info("Redis pool created", "backend_id", poolBackend.id, "max_connections", poolBackend.maxConnections)
	}

	pool.inUse++
	pool.lastUsed = time.Now()
	m.leases.Add(1)

	client := pool.client
	key := poolBackend.key

	m.mu.Unlock()

	var once sync.Once

	release := func() {
		once.Do(func() {
			m.release(key, client)
		})
	}

	return client, release, nil
}

func (m *Manager) Authorized(token string) bool {
	tokenHash := sha256.Sum256([]byte(token))
	_, authorized := m.backends[tokenHash]

	return authorized
}

func (m *Manager) Stats() Stats {
	m.mu.Lock()
	active := len(m.pools)
	m.mu.Unlock()

	return Stats{
		Active:  active,
		Created: m.created.Load(),
		Evicted: m.evicted.Load(),
	}
}

func (m *Manager) Close() error {
	m.mu.Lock()

	if m.closed {
		m.mu.Unlock()
		return nil
	}

	m.closed = true
	close(m.stop)
	m.mu.Unlock()

	<-m.done
	m.leases.Wait()

	m.mu.Lock()
	pools := make([]*managedPool, 0, len(m.pools))

	for _, pool := range m.pools {
		pools = append(pools, pool)
	}

	clear(m.pools)
	m.mu.Unlock()

	var closeErrors []error

	for _, pool := range pools {
		if err := pool.client.Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("close backend %q: %w", pool.backend.id, err))
		}
	}

	return errors.Join(closeErrors...)
}

func (m *Manager) release(key poolKey, client *redis.Client) {
	defer m.leases.Done()

	m.mu.Lock()

	defer m.mu.Unlock()

	pool := m.pools[key]

	if pool == nil || pool.client != client {
		return
	}

	pool.inUse--
	pool.lastUsed = time.Now()
}

func (m *Manager) evictIdlePools() {
	interval := m.idleTimeout / 2

	interval = max(interval, 10*time.Millisecond)
	interval = min(interval, time.Minute)

	ticker := time.NewTicker(interval)

	defer func() {
		ticker.Stop()
		close(m.done)
	}()

	for {
		select {
		case now := <-ticker.C:
			m.evictBefore(now.Add(-m.idleTimeout))

		case <-m.stop:
			return
		}
	}
}

func (m *Manager) evictBefore(cutoff time.Time) {
	m.mu.Lock()
	evicted := make([]*managedPool, 0)

	for key, pool := range m.pools {
		if pool.inUse == 0 && !pool.lastUsed.After(cutoff) {
			delete(m.pools, key)
			evicted = append(evicted, pool)
			m.evicted.Add(1)
		}
	}

	m.mu.Unlock()

	for _, pool := range evicted {
		if err := pool.client.Close(); err != nil {
			m.logger.Warn("Redis pool close failed", "backend_id", pool.backend.id, "error", err)
		}

		m.logger.Info("Redis pool evicted", "backend_id", pool.backend.id, "idle_timeout", m.idleTimeout)
	}
}
