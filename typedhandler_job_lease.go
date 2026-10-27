package allino

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrJobLeaseLost means this execution can no longer mutate the queued job.
// Handlers should stop work when their Runtime.Context is canceled.
var ErrJobLeaseLost = errors.New("job lease lost")

// migrateLeaseToken upgrades existing job tables without deleting queued work.
// Schema inspection also makes the migration idempotent on existing databases.
func (c *callSQLStrategy) leaseColumnExists(ctx context.Context) (bool, error) {
	rows, err := c.db.QueryContext(ctx, "SELECT * FROM executions WHERE 1=0")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return false, err
	}
	for _, name := range columns {
		if name == "lease_token" {
			return true, nil
		}
	}
	return false, nil
}
func (c *callSQLStrategy) migrateLeaseToken(ctx context.Context, allow bool) error {
	exists, err := c.leaseColumnExists(ctx)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if !allow {
		return fmt.Errorf("executions.lease_token is missing: enable sql.allow_migrate to upgrade the job table")
	}
	_, err = c.db.ExecContext(ctx, "ALTER TABLE executions ADD COLUMN lease_token TEXT")
	if err != nil {
		// Another server may have completed the same additive migration.
		if exists, checkErr := c.leaseColumnExists(ctx); checkErr == nil && exists {
			return nil
		}
	}
	return err
}
func requireLeaseMutation(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrJobLeaseLost
	}
	return nil
}
func (t *sqlJobTask) mutateLease(ctx context.Context, operation string, duration time.Duration, delay int) error {
	c := t.strategy
	if c.issqlite {
		c.mu.Lock()
		defer c.mu.Unlock()
	}
	now := time.Now()
	if t.leaseToken == "" {
		return ErrJobLeaseLost
	}
	switch operation {
	case "heartbeat":
		return requireLeaseMutation(c.db.ExecContext(ctx, `UPDATE executions SET leased_until=?,updated_at=?
   WHERE key=? AND status=1 AND lease_token=? AND leased_until>?`, now.Add(duration), now, t.key, t.leaseToken, now))
	case "release":
		return requireLeaseMutation(c.db.ExecContext(ctx, `DELETE FROM executions
   WHERE key=? AND status=1 AND lease_token=? AND leased_until>?`, t.key, t.leaseToken, now))
	case "requeue":
		return requireLeaseMutation(c.db.ExecContext(ctx, `UPDATE executions SET status=0,run_at=?,leased_until=NULL,lease_token=NULL,
   updated_at=?,retry_count=COALESCE(retry_count,0)+1
   WHERE key=? AND status=1 AND lease_token=? AND leased_until>?`, now.Add(time.Duration(delay)*time.Second), now, t.key, t.leaseToken, now))
	default:
		return fmt.Errorf("unknown lease operation %q", operation)
	}
}
