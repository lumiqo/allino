package allino

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// CacheState describes the stored result. Expired results remain readable.
type CacheState string

const (
	CacheMiss            CacheState = "miss"
	CacheFailed          CacheState = "failed"
	CachePending         CacheState = "pending"
	CacheFresh           CacheState = "fresh"
	CacheExpired         CacheState = "expired"
	CacheVersionMismatch CacheState = "version_mismatch"
)

// CacheInfo describes the result at read time. Version mismatch takes precedence
// over expiration for successful results. Miss, failed, and pending states are
// not errors; storage and decoding failures are.
type CacheInfo struct {
	State     CacheState `json:"state"`
	UpdatedAt time.Time  `json:"updated_at,omitempty"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	Version   string     `json:"version,omitempty"`
}

// CallCache reads a saved successful result without executing or enqueueing the
// function, transforming the output, or extending its expiration. Unlike Call,
// it returns expired results. Version-incompatible results are not decoded.
// Successful TTL results are retained one per key, independently of queue cleanup.
func (rw *GenericFunction[T, U, E]) CallCache(r *Runtime, input T) (output U, info CacheInfo, err error) {
	info.State = CacheMiss
	if !r.config.System.DisableValidator {
		if err = r.server.Validator.Struct(input); err != nil {
			return
		}
	}
	if err = r.enforceACL(rw.options, input); err != nil {
		return
	}
	if !rw.options.Job.Cache {
		return output, info, fmt.Errorf("CallCache requires a cache-enabled JobMode")
	}
	c := r.server.callSQLStrategy
	if c == nil {
		return output, info, fmt.Errorf("CallCache requires a configured job store")
	}
	inJSON, err := json.Marshal(input)
	if err != nil {
		return output, info, err
	}
	key := encodeJobID(encodeHandlerName(rw.options), input, inJSON, "")
	var outJSON []byte
	var status int
	// Prefer the saved snapshot. Existing completed rows remain readable before
	// their first post-upgrade refresh, without a data migration.
	err = c.db.QueryRowContext(r.Context(), `
 SELECT output, version, ttl, updated_at, status FROM (
 SELECT output, version, ttl, updated_at, status,
 CASE WHEN status=2 AND error IS NULL THEN 0 ELSE 1 END AS unsuccessful,
 0 AS preference FROM executions_results WHERE key=?
 UNION ALL
 SELECT output, version, ttl, updated_at, status,
 CASE WHEN status=2 AND error IS NULL THEN 0 ELSE 1 END AS unsuccessful,
 1 AS preference FROM executions WHERE key=?
 ) ORDER BY unsuccessful, updated_at DESC, preference LIMIT 1`, key, key).Scan(&outJSON, &info.Version, &info.ExpiresAt, &info.UpdatedAt, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return output, info, nil
	}
	if err != nil {
		return
	}
	if status != statusDone {
		if status == statusError {
			info.State = CacheFailed
		} else {
			info.State = CachePending
		}
		return output, info, nil
	}
	info.State = CacheFresh
	if info.ExpiresAt != nil && !time.Now().Before(*info.ExpiresAt) {
		info.State = CacheExpired
	}
	if hasMajorOrMinorVersionDiff(info.Version, handlerVersion(rw.options)) {
		info.State = CacheVersionMismatch
		return output, info, nil
	}
	err = json.Unmarshal(outJSON, &output)
	return
}

func saveCacheSnapshot(ctx context.Context, tx *sql.Tx, handler string, meta *JobMeta, key string, input, output []byte, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
 INSERT INTO executions_results
 (key, handler, version, status, parentid, rootid, ttl, input, output, error, created_at, updated_at)
 VALUES (?, ?, ?, 2, ?, ?, ?, ?, ?, NULL, ?, ?)
 ON CONFLICT(key) DO UPDATE SET
 handler=excluded.handler, version=excluded.version, status=2,
 parentid=excluded.parentid, rootid=excluded.rootid, ttl=excluded.ttl,
 input=excluded.input, output=excluded.output, error=NULL, updated_at=excluded.updated_at`,
		key, handler, meta.Version, meta.ParentID, meta.RootID, meta.TTL, input, output, now, now)
	return err
}
