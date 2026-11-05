package allino

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// JobResource describes a named resource shared by SQL async/dispatch handlers.
// Callbacks must honor context cancellation and be idempotent: recovery may
// repeat Release after a process dies. All servers using a name must address the
// same underlying resource (for example, the same GPU inference service).
// Use different names for independent devices on different hosts.
type JobResource struct {
	Name        string
	Capacity    int
	IdleTimeout time.Duration
	// Timeout bounds each callback's context; zero defaults to one minute.
	Timeout time.Duration
	Init    func(context.Context) error
	Release func(context.Context) error
}

func (r *JobResource) validate() error {
	if r == nil || r.Name == "" {
		return errors.New("job resource requires a name")
	}
	if r.Capacity < 1 || r.IdleTimeout < 0 || r.Timeout < 0 {
		return fmt.Errorf("invalid job resource policy: %q", r.Name)
	}
	return nil
}
func (r *JobResource) timeout() time.Duration {
	if r.Timeout > 0 {
		return r.Timeout
	}
	return time.Minute
}

type managedResource struct {
	definition JobResource
	source     *JobResource
	done       chan struct{}
}

const resourceTick = 100 * time.Millisecond
const resourceOwnerLease = 30 * time.Second

func (c *callSQLStrategy) migrateManagedResources(ctx context.Context, allow bool) error {
	if allow {
		_, err := c.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS job_managed_resources (
 name TEXT PRIMARY KEY, capacity INTEGER NOT NULL, idle_ns INTEGER NOT NULL, timeout_ns INTEGER NOT NULL,
 state TEXT NOT NULL DEFAULT 'cold', owner TEXT NOT NULL DEFAULT '', owner_until DATETIME NOT NULL,
 idle_since DATETIME, retry_at DATETIME NOT NULL, last_error TEXT NOT NULL DEFAULT ''
 );
 CREATE TABLE IF NOT EXISTS execution_resources (
 job_key TEXT NOT NULL, resource_key TEXT NOT NULL, PRIMARY KEY(job_key,resource_key)
 );
 CREATE INDEX IF NOT EXISTS idx_execution_resources_name ON execution_resources(resource_key,job_key);
 CREATE TRIGGER IF NOT EXISTS executions_resource_delete AFTER DELETE ON executions
 BEGIN DELETE FROM execution_resources WHERE job_key=OLD.key; END;
 CREATE TRIGGER IF NOT EXISTS executions_resource_demand AFTER UPDATE OF status ON executions
 WHEN NEW.status IN (0,1) AND OLD.status<>NEW.status
 BEGIN UPDATE job_managed_resources SET idle_since=NULL
 WHERE name IN (SELECT resource_key FROM execution_resources WHERE job_key=NEW.key); END;`)
		return err
	}
	for _, query := range []string{
		"SELECT name,capacity,idle_ns,timeout_ns,state,owner,owner_until,idle_since,retry_at,last_error FROM job_managed_resources WHERE 1=0",
		"SELECT job_key,resource_key FROM execution_resources WHERE 1=0",
	} {
		rows, err := c.db.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("managed job resource schema is missing: enable sql.allow_migrate: %w", err)
		}
		if err = rows.Close(); err != nil {
			return err
		}
	}
	return nil
}
func registerManagedResource(ctx context.Context, tx *sql.Tx, r *JobResource) error {
	if err := r.validate(); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO job_managed_resources(name,capacity,idle_ns,timeout_ns,owner_until,retry_at)
 VALUES(?,?,?,?,?,?) ON CONFLICT(name) DO NOTHING`, r.Name, r.Capacity, int64(r.IdleTimeout), int64(r.timeout()), time.Unix(0, 0), time.Unix(0, 0))
	if err != nil {
		return err
	}
	var capacity int
	var idle, timeout int64
	if err = tx.QueryRowContext(ctx, "SELECT capacity,idle_ns,timeout_ns FROM job_managed_resources WHERE name=?", r.Name).Scan(&capacity, &idle, &timeout); err != nil {
		return err
	}
	if capacity != r.Capacity || idle != int64(r.IdleTimeout) || timeout != int64(r.timeout()) {
		return fmt.Errorf("%w: %q", ErrResourcePolicyConflict, r.Name)
	}
	return nil
}

func (c *callSQLStrategy) startManagedResources(ctx context.Context, definitions []*JobResource) error {
	if len(definitions) == 0 {
		return nil
	}
	c.managedMu.Lock()
	defer c.managedMu.Unlock()
	if c.managed == nil {
		c.managed = make(map[string]*managedResource)
		c.managedOwner = uuid.NewString()
	}
	// Register all policies before launching callbacks. Definitions are copied so
	// the caller cannot change policy while the lifecycle is running.
	if c.issqlite {
		c.mu.Lock()
		defer c.mu.Unlock()
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	seen := make(map[string]*JobResource)
	for _, r := range definitions {
		if r != nil {
			if previous := seen[r.Name]; previous != nil && previous != r {
				return fmt.Errorf("duplicate job resource definition: %q", r.Name)
			}
			seen[r.Name] = r
		}
		if err := registerManagedResource(ctx, tx, r); err != nil {
			return err
		}
		if old := c.managed[r.Name]; old != nil && (old.definition.Init != nil || old.definition.Release != nil || r.Init != nil || r.Release != nil) {
			// Reuse one definition pointer across handlers; a name cannot select between callbacks.
			// Policy equivalence is checked in SQL; callback identity is checked below.
			if old.source != r {
				return fmt.Errorf("resource %q must reuse the same JobResource definition", r.Name)
			}
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	for _, r := range definitions {
		if c.managed[r.Name] != nil {
			continue
		}
		m := &managedResource{definition: *r, source: r, done: make(chan struct{})}
		c.managed[r.Name] = m
		go c.manageResource(ctx, m)
	}
	return nil
}

// resourceStep owns lifecycle transitions in the database. The UPDATE obtains
// SQLite's writer lock before reading demand, serializing with enqueue/dequeue
// even when another process uses a separate database connection.
func (c *callSQLStrategy) resourceStep(ctx context.Context, r *JobResource, shutting bool) (string, error) {
	if c.issqlite {
		c.mu.Lock()
		defer c.mu.Unlock()
	}
	now := time.Now()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE job_managed_resources SET
 state=CASE WHEN owner<>? AND state<>'cold' THEN 'failed' ELSE state END,
 owner=?,owner_until=?
 WHERE name=? AND (owner=? OR (owner_until<=? AND NOT EXISTS (
 SELECT 1 FROM execution_resources AS held JOIN executions AS active ON active.key=held.job_key
 WHERE held.resource_key=? AND active.status=1 AND active.leased_until>?)))`,
		c.managedOwner, c.managedOwner, now.Add(resourceOwnerLease), r.Name, c.managedOwner, now, r.Name, now)
	if err != nil {
		return "", err
	}
	n, err := result.RowsAffected()
	if err != nil || n == 0 {
		return "", err
	}
	var state string
	var idle sql.NullTime
	var retry time.Time
	if err = tx.QueryRowContext(ctx, "SELECT state,idle_since,retry_at FROM job_managed_resources WHERE name=?", r.Name).Scan(&state, &idle, &retry); err != nil {
		return "", err
	}
	var demand int
	predicate := "(active.status=0 OR active.status=1)"
	if shutting {
		predicate = "(active.status=1 AND active.leased_until>?)"
	}
	args := []any{r.Name}
	if shutting {
		args = append(args, now)
	}
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM execution_resources AS needed JOIN executions AS active ON active.key=needed.job_key WHERE needed.resource_key=? AND `+predicate, args...).Scan(&demand)
	if err != nil {
		return "", err
	}
	action := ""
	switch state {
	case "cold":
		if demand > 0 && !shutting && !now.Before(retry) {
			action = "initializing"
		}
	case "failed":
		if !now.Before(retry) {
			action = "releasing"
		}
	case "ready":
		if demand > 0 {
			_, err = tx.ExecContext(ctx, "UPDATE job_managed_resources SET idle_since=NULL WHERE name=?", r.Name)
		} else if shutting || (idle.Valid && !now.Before(idle.Time.Add(r.IdleTimeout))) {
			action = "releasing"
		} else if !idle.Valid {
			_, err = tx.ExecContext(ctx, "UPDATE job_managed_resources SET idle_since=? WHERE name=?", now, r.Name)
		}
	}
	if err != nil {
		return "", err
	}
	if action != "" {
		_, err = tx.ExecContext(ctx, "UPDATE job_managed_resources SET state=?,idle_since=NULL WHERE name=?", action, r.Name)
		if err != nil {
			return "", err
		}
	}
	if err = tx.Commit(); err != nil {
		return "", err
	}
	return action, nil
}

func resourceCallback(ctx context.Context, fn func(context.Context) error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("resource callback panic: %v", p)
		}
	}()
	if fn != nil {
		err = fn(ctx)
	}
	if err == nil {
		err = ctx.Err()
	}
	return
}
func (c *callSQLStrategy) finishResource(ctx context.Context, r *JobResource, action string, callbackErr error) error {
	if c.issqlite {
		c.mu.Lock()
		defer c.mu.Unlock()
	}
	state := "ready"
	if action == "releasing" {
		state = "cold"
	}
	message := ""
	retry := time.Unix(0, 0)
	if callbackErr != nil {
		state = "failed"
		message = callbackErr.Error()
		retry = time.Now().Add(time.Second)
	}
	return requireLeaseMutation(c.db.ExecContext(ctx, `UPDATE job_managed_resources SET state=?,last_error=?,retry_at=?,idle_since=NULL
 WHERE name=? AND owner=? AND owner_until>? AND state=?`, state, message, retry, r.Name, c.managedOwner, time.Now(), action))
}
func (c *callSQLStrategy) manageResource(appctx context.Context, m *managedResource) {
	defer close(m.done)
	r := &m.definition
	ticker := time.NewTicker(resourceTick)
	defer ticker.Stop()
	var result chan error
	var action string
	var cancel context.CancelFunc
	var completionPending bool
	var completedErr error
	var shutdownAt time.Time
	for {
		shutting := appctx.Err() != nil
		if shutting && shutdownAt.IsZero() {
			shutdownAt = time.Now()
			if cancel != nil {
				cancel()
			}
		}
		// Never unload while a valid execution lease still exists. If shutdown cannot
		// drain, leave ownership to expire so another server performs recovery.
		if shutting && result == nil && !completionPending && time.Since(shutdownAt) > resourceOwnerLease {
			return
		}
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		next, err := c.resourceStep(ctx, r, shutting)
		if completionPending && err == nil {
			err = c.finishResource(ctx, r, action, completedErr)
			if err == nil || errors.Is(err, ErrJobLeaseLost) {
				completionPending = false
			}
		}
		stop()
		if err != nil && c.logger != nil {
			c.logger.Error("job resource maintenance failed", zap.String("resource", r.Name), zap.Error(err))
		}
		if next != "" && result == nil && !completionPending {
			action = next
			result = make(chan error, 1)
			callbackCtx, callbackCancel := context.WithTimeout(context.Background(), r.timeout())
			cancel = callbackCancel
			fn := r.Init
			if action == "releasing" {
				fn = r.Release
			}
			go func(ch chan error) { defer callbackCancel(); ch <- resourceCallback(callbackCtx, fn) }(result)
		}
		if shutting && result == nil && !completionPending {
			var state string
			err := c.db.QueryRow("SELECT state FROM job_managed_resources WHERE name=? AND owner=?", r.Name, c.managedOwner).Scan(&state)
			if errors.Is(err, sql.ErrNoRows) {
				return
			}
			if err == nil && state == "cold" {
				// Hand ownership over immediately after clean shutdown, rather
				// than making another server wait for the owner lease to expire.
				ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
				_, err = c.db.ExecContext(ctx, "UPDATE job_managed_resources SET owner='',owner_until=? WHERE name=? AND owner=? AND state='cold'", time.Now(), r.Name, c.managedOwner)
				stop()
				if err == nil {
					return
				}
			}
		}
		select {
		case callbackErr := <-result:
			cancel()
			cancel = nil
			result = nil
			ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			err := c.finishResource(ctx, r, action, callbackErr)
			if err != nil && !errors.Is(err, ErrJobLeaseLost) {
				completionPending = true
				completedErr = callbackErr
			}
			stop()
			if callbackErr != nil && c.logger != nil {
				c.logger.Error("job resource callback failed", zap.String("resource", r.Name), zap.String("action", action), zap.Error(callbackErr))
			}
			if err != nil && c.logger != nil {
				c.logger.Error("job resource transition failed", zap.String("resource", r.Name), zap.Error(err))
			}
		case <-ticker.C:
		}
	}
}

// waitManagedResources is called after worker cancellation during server shutdown.
func (c *callSQLStrategy) waitManagedResources() error {
	c.managedMu.Lock()
	resources := make([]*managedResource, 0, len(c.managed))
	timeout := resourceOwnerLease + time.Second
	for _, m := range c.managed {
		resources = append(resources, m)
		if d := resourceOwnerLease + m.definition.timeout() + time.Second; d > timeout {
			timeout = d
		}
	}
	c.managedMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for _, m := range resources {
		select {
		case <-m.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
