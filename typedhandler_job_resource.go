package allino

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ResourceRequest identifies a scheduling resource shared across job handlers.
// Return an empty key to opt an input out of resource scheduling.
// Resource keys do not change the job's identity or cached result key.
type ResourceRequest interface{ ResourceKey() string }

var ErrResourcePolicyConflict = errors.New("conflicting job resource policy")
var errResourceDeferred = errors.New("job resource start deferred")

func resourceLimitsEnabled(opt JobOption) bool {
	return opt.MinInterval > 0 || opt.MaxConcurrentPerResource > 0
}
func resourceSchedulingEnabled(opt JobOption) bool {
	return resourceLimitsEnabled(opt) || len(opt.Resources) > 0
}
func validateResourceOptions(opt *Option) error {
	for _, resource := range opt.Job.Resources {
		if err := resource.validate(); err != nil {
			return err
		}
	}
	if opt.Job.MinInterval < 0 || opt.Job.MaxConcurrentPerResource < 0 {
		return errors.New("resource limits must not be negative")
	}
	if resourceSchedulingEnabled(opt.Job) && opt.JobMode != JOBMODE_ASYNC && opt.JobMode != JOBMODE_DISPATCH {
		return errors.New("resource scheduling requires async or dispatch job mode")
	}
	return nil
}
func getResourceKey(input any) (string, bool) {
	r, ok := input.(ResourceRequest)
	if !ok {
		return "", false
	}
	return r.ResourceKey(), true
}

func (c *callSQLStrategy) migrateJobResources(ctx context.Context, allow bool) error {
	for _, column := range []struct{ name, definition string }{
		{"user_context", "TEXT"},
		{"resource_key", "TEXT NOT NULL DEFAULT ''"},
		{"resource_started_at", "DATETIME"},
	} {
		found, err := c.executionColumnExists(ctx, column.name)
		if err != nil {
			return err
		}
		if !found {
			if !allow {
				return fmt.Errorf("executions.%s is missing: enable sql.allow_migrate", column.name)
			}
			_, err = c.db.ExecContext(ctx, "ALTER TABLE executions ADD COLUMN "+column.name+" "+column.definition)
			if err != nil {
				exists, checkErr := c.executionColumnExists(ctx, column.name)
				if checkErr != nil || !exists {
					return err
				}
			}
		}
	}
	if allow {
		_, err := c.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS job_resources (
   resource_key TEXT PRIMARY KEY,
   min_interval_ns INTEGER NOT NULL,
   max_concurrent INTEGER NOT NULL,
   next_start_at DATETIME NOT NULL
  ); CREATE INDEX IF NOT EXISTS idx_exec_resource_lease ON executions(resource_key,status,leased_until);`)
		if err != nil {
			return err
		}
		return c.migrateManagedResources(ctx, true)
	}
	rows, err := c.db.QueryContext(ctx, "SELECT resource_key,min_interval_ns,max_concurrent,next_start_at FROM job_resources WHERE 1=0")
	if err != nil {
		return fmt.Errorf("job_resources schema is missing: enable sql.allow_migrate: %w", err)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	return c.migrateManagedResources(ctx, false)
}
func registerResourcePolicy(ctx context.Context, tx *sql.Tx, meta *JobMeta, now time.Time) error {
	if meta.ResourceKey == "" {
		return nil
	}
	if meta.resourceMinInterval < 0 || meta.resourceMaxConcurrent < 0 {
		return errors.New("resource limits must not be negative")
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO job_resources(resource_key,min_interval_ns,max_concurrent,next_start_at)
 VALUES(?,?,?,?) ON CONFLICT(resource_key) DO NOTHING`, meta.ResourceKey, int64(meta.resourceMinInterval), meta.resourceMaxConcurrent, now)
	if err != nil {
		return err
	}
	var interval int64
	var concurrency int
	if err = tx.QueryRowContext(ctx, "SELECT min_interval_ns,max_concurrent FROM job_resources WHERE resource_key=?", meta.ResourceKey).Scan(&interval, &concurrency); err != nil {
		return err
	}
	if interval != int64(meta.resourceMinInterval) || concurrency != meta.resourceMaxConcurrent {
		return fmt.Errorf("%w: %q", ErrResourcePolicyConflict, meta.ResourceKey)
	}
	return nil
}

// Start confirms ownership and opens the resource's next interval immediately
// before invoking the handler, after any legacy per-handler interval delay.
func (t *sqlJobTask) Start(ctx context.Context) error {
	c := t.strategy
	if c.issqlite {
		c.mu.Lock()
		defer c.mu.Unlock()
	}
	if t.leaseToken == "" {
		return ErrJobLeaseLost
	}
	now := time.Now()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE executions SET resource_started_at=?
 WHERE key=? AND status=1 AND lease_token=? AND leased_until>? AND resource_started_at IS NULL`, now, t.key, t.leaseToken, now)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		// An already started, still owned task is idempotent; stale attempts fail.
		var count int
		err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM executions WHERE key=? AND status=1 AND lease_token=?
   AND leased_until>? AND resource_started_at IS NOT NULL`, t.key, t.leaseToken, now).Scan(&count)
		if err != nil {
			return err
		}
		if count != 1 {
			return ErrJobLeaseLost
		}
		return tx.Commit()
	}
	var key string
	if err = tx.QueryRowContext(ctx, "SELECT resource_key FROM executions WHERE key=?", t.key).Scan(&key); err != nil {
		return err
	}
	if key != "" {
		var interval int64
		var next time.Time
		if err = tx.QueryRowContext(ctx, "SELECT min_interval_ns,next_start_at FROM job_resources WHERE resource_key=?", key).Scan(&interval, &next); err != nil {
			return err
		}
		if now.Before(next) {
			// A clock change or administrative policy update may move eligibility after
			// dequeue. Return to the queue without consuming an application retry.
			_, err = tx.ExecContext(ctx, `UPDATE executions SET status=0,run_at=?,lease_token=NULL,leased_until=NULL,resource_started_at=NULL WHERE key=?`, next, t.key)
			if err != nil {
				return err
			}
			if err = tx.Commit(); err != nil {
				return err
			}
			return errResourceDeferred
		}
		if _, err = tx.ExecContext(ctx, "UPDATE job_resources SET next_start_at=? WHERE resource_key=?", now.Add(time.Duration(interval)), key); err != nil {
			return err
		}
	}
	return tx.Commit()
}
