package allino

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/wh-kuromai/allino/internal/ema"
	"github.com/wh-kuromai/allino/internal/timewheel"
	"go.uber.org/zap"
)

const (
	statusQueued int = iota
	statusLeased
	statusDone
	statusError
	statusSize
)

var sqlStatusCodeStrings = []string{
	"queued",
	"leased",
	"done",
	"error",
}

type functionInvoker = func(r *Runtime, handler, version string, injson []byte, direct bool, infunc func(input any) error) (key string, outjson []byte, err []byte, syserr error)

type callSQLStrategy struct {
	name     string
	db       *sql.DB
	issqlite bool
	mu       sync.Mutex

	lastDequeuedId atomic.Int64
	waitInterval   time.Duration
	waitTimeout    time.Duration
	maxretry       int
	logger         *zap.Logger
}

type sqlJobTask struct {
	leaseToken string
	strategy   *callSQLStrategy
	key        string
	handler    string
	meta       *JobMeta
	input      []byte
}

func (t *sqlJobTask) Key() string {
	return t.key
}
func (t *sqlJobTask) Handler() string {
	return t.handler
}
func (t *sqlJobTask) Meta() *JobMeta {
	return t.meta

}
func (t *sqlJobTask) Input() []byte {
	return t.input
}
func (t *sqlJobTask) Success(ctx context.Context, handler string, meta *JobMeta, key string, injson []byte, outjson []byte, errjson []byte) (err error) {
	if t.leaseToken == "" || key != t.key {
		return ErrJobLeaseLost
	}
	return t.strategy.doneAsyncWithLease(ctx, handler, meta, key, injson, outjson, errjson, t.leaseToken)
}
func (t *sqlJobTask) Fail(ctx context.Context) (err error) {
	return t.mutateLease(ctx, "release", 0, 0)
}
func (t *sqlJobTask) HeartBeat(ctx context.Context, lease_dur time.Duration) (err error) {
	return t.mutateLease(ctx, "heartbeat", lease_dur, 0)
}
func (t *sqlJobTask) Requeue(ctx context.Context, delay_sec int) error {
	return t.mutateLease(ctx, "requeue", 0, delay_sec)
}

func newcallSQLStrategy(sv *Server) *callSQLStrategy {
	s := &callSQLStrategy{
		name:         "sql",
		db:           sv.SQL,
		issqlite:     false,
		waitInterval: sv.Config.JobConfig.WaitInterval,
		waitTimeout:  sv.Config.JobConfig.WaitTimeout,
		maxretry:     sv.Config.JobConfig.MaxRetry,
		logger:       sv.Logger,
	}

	s.lastDequeuedId.Store(-1)
	return s
}

func isSQLite(db *sql.DB) bool {
	var v string
	err := db.QueryRow("SELECT sqlite_version()").Scan(&v)
	return err == nil
}

func (c *callSQLStrategy) Init(ctx context.Context, allow_migrate bool) error {
	if isSQLite(c.db) {
		c.issqlite = true
	}

	if c.issqlite {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	if !allow_migrate {
		return c.migrateLeaseToken(ctx, false)
	}

	// 0:queued 1:leased 2:done 3:error
	_, err := c.db.Exec(`
CREATE TABLE IF NOT EXISTS executions (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	key TEXT UNIQUE,
	handler TEXT,
	version TEXT,
	status INTEGER NOT NULL,          -- queued / leased / done / error / dead
	parentid TEXT,
	rootid TEXT,
	input BLOB,
	output BLOB,
	error BLOB,
	retry_count INTEGER,
	priority INTEGER NOT NULL,
	ttl DATETIME,
	run_at DATETIME NOT NULL,
	created_at DATETIME NOT NULL,
	updated_at DATETIME NOT NULL,
	leased_until DATETIME,
 lease_token TEXT
);

CREATE INDEX IF NOT EXISTS idx_exec_queued
ON executions (handler, priority DESC, created_at ASC)
WHERE status = 0; -- queued

CREATE INDEX IF NOT EXISTS idx_exec_lease
ON executions(leased_until)
WHERE status = 1; -- leased

-- CREATE INDEX idx_exec_key
-- ON executions(key);

-- COUNTING

CREATE TABLE IF NOT EXISTS execution_counts (
	rootid TEXT,
	status TEXT,
	count INTEGER NOT NULL,
	PRIMARY KEY (rootid, status)
);

-- INSERT トリガー
CREATE TRIGGER IF NOT EXISTS executions_after_insert
AFTER INSERT ON executions
BEGIN
  -- 全体カウント (rootid を '*' とする)
  INSERT INTO execution_counts(rootid, status, count)
  VALUES ('*', NEW.status, 1)
  ON CONFLICT(rootid, status) DO UPDATE SET count = count + 1;

  -- rootid ごとのカウント (NULL なら '*' なので上記と重複するため、NULL でない時のみ)
  INSERT INTO execution_counts(rootid, status, count)
  SELECT NEW.rootid, NEW.status, 1
  WHERE NEW.rootid IS NOT NULL
  ON CONFLICT(rootid, status) DO UPDATE SET count = count + 1;
END;

-- UPDATE トリガー
CREATE TRIGGER IF NOT EXISTS executions_after_update
AFTER UPDATE ON executions
WHEN OLD.status != NEW.status OR IFNULL(OLD.rootid, '') != IFNULL(NEW.rootid, '')
BEGIN
  -- 古い状態を減算 (全体)
  UPDATE execution_counts SET count = count - 1 WHERE rootid = '*' AND status = OLD.status;
  -- 古い状態を減算 (rootid別)
  UPDATE execution_counts SET count = count - 1 WHERE rootid = OLD.rootid AND status = OLD.status AND OLD.rootid IS NOT NULL;

  -- 新しい状態を加算 (全体)
  INSERT INTO execution_counts(rootid, status, count)
  VALUES ('*', NEW.status, 1)
  ON CONFLICT(rootid, status) DO UPDATE SET count = count + 1;

  -- 新しい状態を加算 (rootid別)
  INSERT INTO execution_counts(rootid, status, count)
  SELECT NEW.rootid, NEW.status, 1
  WHERE NEW.rootid IS NOT NULL
  ON CONFLICT(rootid, status) DO UPDATE SET count = count + 1;
END;

-- DELETE トリガー
CREATE TRIGGER IF NOT EXISTS executions_after_delete
AFTER DELETE ON executions
BEGIN
  UPDATE execution_counts SET count = count - 1 WHERE rootid = '*' AND status = OLD.status;
  UPDATE execution_counts SET count = count - 1 WHERE rootid = OLD.rootid AND status = OLD.status AND OLD.rootid IS NOT NULL;
END;


CREATE TABLE IF NOT EXISTS executions_results (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	key TEXT UNIQUE,
	handler TEXT,
	version TEXT,
	status INTEGER NOT NULL,          -- queued / leased / done / error
	parentid TEXT,
	rootid TEXT,
	ttl DATETIME,
	input BLOB,
	output BLOB,
	error BLOB,
	created_at DATETIME NOT NULL,
	updated_at DATETIME NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_exec_result_key
ON executions(key);
	`)

	if err != nil {
		return err
	}
	return c.migrateLeaseToken(ctx, true)
}

func (c *callSQLStrategy) Enqueue(
	ctx context.Context,
	handler string,
	meta *JobMeta,
	key string,
	injson []byte,
	delay_sec int,
) (bool, error) {
	if c.issqlite {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	now := time.Now()

	//res, err := c.db.ExecContext(ctx, `
	//INSERT OR IGNORE INTO executions
	//(key, handler, version, status, parentid, rootid, priority, ttl, created_at, updated_at, run_at, input)
	//VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	//`, key, handler, meta.Version, meta.Status, meta.ParentID, meta.RootID, meta.Priority, meta.TTL, now, now, now, injson)

	// 0:queued 1:leased 2:done 3:error
	res, err := c.db.ExecContext(ctx, `
INSERT INTO executions
(key, handler, version, status, parentid, rootid, priority, ttl, created_at, updated_at, run_at, input)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

ON CONFLICT(key) DO UPDATE SET
	handler=excluded.handler,
	version=excluded.version,
	status=excluded.status,
 lease_token=NULL,
 leased_until=NULL,
	parentid=excluded.parentid,
	rootid=excluded.rootid,
	priority=excluded.priority,
	ttl=excluded.ttl,
	run_at=excluded.run_at,
	input=excluded.input,
	updated_at=excluded.updated_at

WHERE executions.status=2 -- done
  AND executions.ttl < ?
`, key, handler, meta.Version, meta.Status, meta.ParentID, meta.RootID,
		meta.Priority, meta.TTL, now, now, now, injson, now)

	if err != nil {
		return false, err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}

	if rows > 0 {
		return true, nil
	}

	return false, nil
}

func (c *callSQLStrategy) Dequeue(
	ctx context.Context,
	handlers []string,
	leaseDuration time.Duration,
	ema *ema.EMACalculator,
) (jt JobTask, err error) {
	var key string
	var handler string
	var meta JobMeta
	var injson []byte

	if c.issqlite {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	if len(handlers) == 0 {
		return nil, ErrJobNotFound
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	now := time.Now()
	leaseUntil := now.Add(leaseDuration)
	leaseToken := uuid.NewString()

	// IN (?, ?, ...) を作る
	placeholders := make([]string, len(handlers))
	for i := range handlers {
		placeholders[i] = "?"
	}

	// args は SQL の ? の順番に合わせる
	// SET leased_until=?, updated_at=?, ... run_at<=?, leased_until<?, handler IN (...)
	args := make([]any, 0, 4+len(handlers))
	args = append(args,
		leaseUntil, // SET leased_until = ?
		leaseToken, // SET lease_token = ?
		now,        // SET updated_at = ?
		now,        // run_at <= ?
		//now,        // leased_until < ?
		//now,        // ttl < ?
	)
	for _, h := range handlers {
		args = append(args, h) // handler IN (...)
	}

	//optimizeWhere := ""

	//if !c.issqlite {
	//	optimizeWhere = "FOR UPDATE SKIP LOCKED"
	//}

	// 0:queued 1:leased 2:done 3:error
	// ✅ UPDATE で 1件だけ lease して、その行を RETURNING で回収（これが超重要）
	q := fmt.Sprintf(`
UPDATE executions
SET
  status = 1, -- leased
  leased_until = ?,
  lease_token = ?,
  updated_at = ?
WHERE id = (
  SELECT id
  FROM executions
  WHERE status = 0 AND run_at <= ? AND handler IN (%s) -- queued
  ORDER BY priority DESC, created_at ASC, id ASC
  LIMIT 1
)
RETURNING
  id, key, handler, version, status, parentid, rootid, input
`, strings.Join(placeholders, ","))

	row := tx.QueryRowContext(ctx, q, args...)
	var id int64
	if err := row.Scan(
		&id,
		&key,
		&handler,
		&meta.Version,
		&meta.Status,
		&meta.ParentID,
		&meta.RootID,
		&injson,
	); err != nil {

		// UPDATE が 0件なら RETURNING も 0行 -> sql.ErrNoRows
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrJobNotFound
		}
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	// Dequeue
	var cidv int64
	lid := c.lastDequeuedId.Load()
	if lid < 0 {
		cidv = 1
	} else {
		cidv = id - lid
	}

	ema.Update(float64(cidv))
	c.lastDequeuedId.Store(id)

	return &sqlJobTask{
		leaseToken: leaseToken,
		strategy:   c,
		key:        key,
		handler:    handler,
		meta:       &meta,
		input:      injson,
	}, nil
	//return key, handler, meta, injson, nil
}

func (c *callSQLStrategy) Reaping(ctx context.Context) error {
	if c.issqlite {
		c.mu.Lock()
		defer c.mu.Unlock()
	}
	now := time.Now()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `UPDATE executions SET status=3,
 error='{"message":"max retries exceeded during reaping"}',updated_at=?,lease_token=NULL
 WHERE status=1 AND leased_until<? AND COALESCE(retry_count,0)>=?`, now, now, c.maxretry)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE executions SET status=0,leased_until=NULL,lease_token=NULL,
 updated_at=?,retry_count=COALESCE(retry_count,0)+1 WHERE status=1 AND leased_until<?`, now, now)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM executions WHERE status IN (2,3) AND ttl IS NOT NULL AND ttl<?`, now)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (c *callSQLStrategy) LeaseUpdate(ctx context.Context, key string, lease_dur time.Duration) (err error) {
	if c.issqlite {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	now := time.Now()
	leaset := now.Add(lease_dur)
	_, err = c.db.ExecContext(ctx, `
	UPDATE executions
	SET 
  	leased_until = ?,
		updated_at = ?
	WHERE key = ?
	`, leaset, now, key)

	return err
}

func (c *callSQLStrategy) doneAsyncWithLease(
	ctx context.Context,
	handler string,
	meta *JobMeta,
	key string,
	injson []byte,
	outjson []byte,
	errjson []byte,
	leaseToken string,
) error {
	if c.issqlite {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	status := statusDone
	if errjson != nil {
		status = statusError
	}

	now := time.Now()

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	if leaseToken == "" {
		return ErrJobLeaseLost
	}
	guard := " WHERE key=? AND status=1 AND lease_token=? AND leased_until>?"
	guardArgs := []any{key, leaseToken, now}
	if meta.TTL != nil {
		args := append([]any{status, meta.TTL, outjson, errjson, now}, guardArgs...)
		result, updateErr := tx.ExecContext(ctx, `UPDATE executions SET status=?,ttl=?,output=?,error=?,updated_at=?,lease_token=NULL,leased_until=NULL`+guard, args...)
		err = requireLeaseMutation(result, updateErr)
		if err != nil {
			return err
		}
	} else {
		args := append([]any{status, meta.TTL, now}, guardArgs...)
		result, updateErr := tx.ExecContext(ctx, `UPDATE executions SET status=?,ttl=?,updated_at=?,lease_token=NULL,leased_until=NULL`+guard, args...)
		err = requireLeaseMutation(result, updateErr)
		if err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, `
	INSERT INTO executions_results
	(key, handler, version, status, parentid, rootid, ttl, input, output, error, created_at, updated_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(key) DO UPDATE SET
		output = excluded.output,
		error = excluded.error,
		status = excluded.status,
		updated_at = excluded.updated_at
	`, key, handler, meta.Version, status, meta.ParentID, meta.RootID, meta.TTL, injson, outjson, errjson, now, now)
	}
	//a, b := res.RowsAffected()
	//fmt.Println("doneasync", a, b)
	//
	//c.List(ctx, []string{"done"}, 20, 0)

	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (c *callSQLStrategy) Free(
	ctx context.Context,
	key string,
) error {
	if c.issqlite {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	_, err := c.db.ExecContext(ctx, `
	DELETE FROM executions 
	WHERE key = ?
	`, key)

	return err
}

func (c *callSQLStrategy) Result(
	ctx context.Context,
	key string,
	volatile bool,
) (JobInfo, []byte, []byte, error) {

	var row *sql.Row
	if volatile {
		row = c.db.QueryRowContext(ctx, `
	SELECT key, handler, version, status, parentid, rootid, created_at, updated_at, output, error
	FROM executions
	WHERE key = ?
	`, key)
	} else {
		row = c.db.QueryRowContext(ctx, `
	SELECT key, handler, version, status, parentid, rootid, created_at, updated_at, output, error
	FROM executions_results
	WHERE key = ?
	`, key)
	}

	var out, errb []byte
	var ji JobInfo
	if err := row.Scan(
		&ji.JobID,
		&ji.Handler,
		&ji.Meta.Version,
		&ji.Meta.Status,
		&ji.Meta.ParentID,
		&ji.Meta.RootID,
		&ji.CreatedAt,
		&ji.UpdatedAt,
		&out,
		&errb,
	); err != nil {
		c.logger.Info(err.Error())
		return ji, nil, nil, ErrJobNotFound
	}

	if ji.Meta.Status != statusDone && ji.Meta.Status != statusError {
		return ji, nil, nil, NewJobPendingError(key, "job not finished yet")
	}

	return ji, out, errb, nil
}

func (c *callSQLStrategy) Wait(
	ctx context.Context,
	key string,
	volatile bool,
	tw *timewheel.TimeWheel,
) (ji JobInfo, output []byte, err []byte, syserr error) {
	var zeroJ JobInfo

	now := time.Now()

	done := make(chan bool, 1)
	tw.Add(c.waitInterval, func() bool {

		ji, output, err, syserr = c.Result(ctx, key, volatile)
		if syserr != nil {
			return true
		}

		if ji.Meta.Status == statusDone || ji.Meta.Status == statusError {
			done <- true
			return false
		}

		// lease expired
		if ji.Meta.Status == statusLeased &&
			ji.LeasedUntil != nil &&
			time.Now().After(*ji.LeasedUntil) {
			ji, output, err, syserr = zeroJ, nil, nil, ErrJobExpired
			done <- true
			return false
		}

		if time.Now().After(now.Add(c.waitTimeout)) {
			ji, output, err, syserr = zeroJ, nil, nil, ErrJobExpired
			done <- true
			return false
		}

		return true
	})

	<-done
	return
}

func (c *callSQLStrategy) Hit(
	ctx context.Context,
	handler string,
	volatile bool,
	key string,
	injson []byte,
) (JobInfo, []byte, []byte, error) {

	var row *sql.Row
	if volatile {
		row = c.db.QueryRowContext(ctx, `
	SELECT key, handler, version, status, parentid, rootid, ttl, output, error, created_at, updated_at
	FROM executions
	WHERE key = ?
	`, key)
	} else {
		row = c.db.QueryRowContext(ctx, `
	SELECT key, handler, version, status, parentid, rootid, ttl, output, error, created_at, updated_at
	FROM executions_results
	WHERE key = ?
	`, key)
	}

	var ji JobInfo
	var out, errb []byte
	if err := row.Scan(
		&ji.JobID,
		&ji.Handler,
		&ji.Meta.Version,
		&ji.Meta.Status,
		&ji.Meta.ParentID,
		&ji.Meta.RootID,
		&ji.Meta.TTL,
		&out,
		&errb,
		&ji.CreatedAt,
		&ji.UpdatedAt,
	); err != nil {
		return ji, nil, nil, ErrJobNotFound
	}

	if ji.Meta.Status != statusDone && ji.Meta.Status != statusError {
		return ji, nil, nil, NewJobPendingError(key, "job not finished yet")
	}

	//now := time.Now()
	//if ji.Meta.TTL != nil && now.After(*ji.Meta.TTL) {
	//	return ji, nil, nil, ErrJobNotFound
	//}

	return ji, out, errb, nil
}

func (c *callSQLStrategy) Done(
	ctx context.Context,
	handler string,
	meta *JobMeta,
	key string,
	injson []byte,
	outjson []byte,
	errjson []byte,
) error {
	if c.issqlite {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	status := statusDone
	if errjson != nil {
		status = statusError
	}

	now := time.Now()

	var err error
	if injson != nil {

	}

	if meta.TTL != nil {
		_, err = c.db.ExecContext(ctx, `
	INSERT INTO executions
	(key, handler, version, status, parentid, rootid, ttl, input, output, error, created_at, updated_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(key) DO UPDATE SET
		output = excluded.output,
		error = excluded.error,
		status = excluded.status,
		updated_at = excluded.updated_at
	`, key, handler, meta.Version, status, meta.ParentID, meta.RootID, meta.TTL, injson, outjson, errjson, now, now)

	} else {
		_, err = c.db.ExecContext(ctx, `
	INSERT INTO executions_results
	(key, handler, version, status, parentid, rootid, ttl, input, output, error, created_at, updated_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(key) DO UPDATE SET
		output = excluded.output,
		error = excluded.error,
		status = excluded.status,
		updated_at = excluded.updated_at
	`, key, handler, meta.Version, status, meta.ParentID, meta.RootID, meta.TTL, injson, outjson, errjson, now, now)

	}

	return err
}

func (c *callSQLStrategy) List(
	ctx context.Context,
	statuses []int,
	limit, offset int,
) ([]JobInfo, error) {

	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	where := ""
	args := make([]any, 0, len(statuses)+2)

	if len(statuses) > 0 {
		placeholders := make([]string, len(statuses))
		for i, st := range statuses {
			placeholders[i] = "?"
			args = append(args, st)
		}
		where = "WHERE status IN (" + strings.Join(placeholders, ",") + ")"
	}

	args = append(args, limit, offset)

	query := `
	SELECT key, handler, version, status, parentid, rootid, priority, ttl, created_at, updated_at, leased_until, retry_count
	FROM executions
	` + where + `
	ORDER BY created_at DESC
	LIMIT ?
	OFFSET ?
	`

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobinfos := make([]JobInfo, 0, limit)

	for rows.Next() {
		var ji JobInfo
		if err := rows.Scan(
			&ji.JobID,
			&ji.Handler,
			&ji.Meta.Version,
			&ji.Meta.Status,
			&ji.Meta.ParentID,
			&ji.Meta.RootID,
			&ji.Meta.Priority,
			&ji.Meta.TTL,
			&ji.CreatedAt,
			&ji.UpdatedAt,
			&ji.LeasedUntil,
			&ji.RetryCount,
		); err != nil {
			return nil, err
		}
		jobinfos = append(jobinfos, ji)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	//b, _ := json.MarshalIndent(jobinfos, "", "  ")
	//fmt.Println(string(b))

	return jobinfos, nil
}

func (c *callSQLStrategy) requeue(ctx context.Context, key string, delay_sec int) error {
	if c.issqlite {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	now := time.Now()
	runAt := now.Add(time.Duration(delay_sec) * time.Second)

	_, err := c.db.ExecContext(ctx, `
  UPDATE executions
  SET 
    status = 0, -- queued
    run_at = ?,
    leased_until = NULL,
    updated_at = ?,
		retry_count = COALESCE(retry_count, 0) + 1
  WHERE key = ?
  `, runAt, now, key)

	return err
}

func (c *callSQLStrategy) Total(ctx context.Context, jobid ...string) (map[string]int, error) {
	if c.issqlite {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	var rows *sql.Rows
	var err error
	if len(jobid) == 0 {
		query := `SELECT status, count FROM execution_counts WHERE rootid = '*';`
		rows, err = c.db.QueryContext(ctx, query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
	} else {
		query := `SELECT status, count FROM execution_counts WHERE rootid = ?;`
		rows, err = c.db.QueryContext(ctx, query, jobid[0])
		if err != nil {
			return nil, err
		}
		defer rows.Close()
	}

	result := make(map[string]int)

	for rows.Next() {
		var status string
		var count int

		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}

		result[status] = count
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}
