package revoker

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wh-kuromai/allino"
	"github.com/wh-kuromai/allino/internal/trie"
	"github.com/wh-kuromai/cryptino"
)

var revoker = NewRevoker()

var RevokerExtension = allino.NewExtension[any, any](
	"revoker",
	&allino.ExtOption{
		OnInit: initializeSQLCookies,
		OnAuthZ: func(r *allino.Runtime, jwt *cryptino.JSONWebToken) (*cryptino.JSONWebToken, error) {
			if r.Config().Login.Revoke.UseSQLCookieRevoke {
				if err := checkSQLCookie(r, jwt); err != nil {
					return nil, err
				}
			}
			if r.Config().Login.Revoke.UseLoginRevoke {
				revoked, reason := revoker.IsRevoked(jwt.Body.Subject, time.Unix(jwt.Body.IssuedAt, 0))
				if revoked {
					return nil, allino.NewError(reason)
				}
			}

			return jwt, nil
		},
	},
)

type RevokeInput struct {
	Scope  string    `json:"scope"`
	Time   time.Time `json:"time"`
	Reason string    `json:"reason"`
}

type RevokeOutput struct {
}

var RevokeHandler = allino.NewFunction(
	allino.Option{
		Name:    "revoker",
		Version: "1.0.0",
		JobMode: "fanout",
	},
	func(r *allino.Runtime, param *RevokeInput) (*RevokeOutput, error) {
		revoker.Insert(param.Scope, param.Time, param.Reason)
		return nil, nil
	},
)

var RevokePrefixHandler = allino.NewFunction(
	allino.Option{
		Name:    "revoker-prefix",
		Version: "1.0.0",
		JobMode: "fanout",
	},
	func(r *allino.Runtime, param *RevokeInput) (*RevokeOutput, error) {
		revoker.InsertPrefix(param.Scope, param.Time, param.Reason)
		return nil, nil
	},
)

type Revoker struct {
	mu                   sync.RWMutex
	revokeRotateInterval time.Duration
	revokeKeepBuckets    int
	revokeBuckets        atomic.Pointer[[]revokeBucket]
	revokeMaps           atomic.Pointer[[]revokeMap]
}

type revokeSet struct {
	when   int64
	reason string
}

type revokeMap struct {
	since time.Time
	kvs   map[string]revokeSet
}

type revokeBucket struct {
	since time.Time
	trie  *trie.Trie[revokeSet]
}

func NewRevoker() *Revoker {
	revoker := &Revoker{
		revokeRotateInterval: time.Hour,
		revokeKeepBuckets:    24,
	}
	buckets := []revokeBucket{
		{
			since: time.Now(),
			trie:  trie.NewTrie[revokeSet](),
		},
	}
	revoker.revokeBuckets.Store(&buckets)

	rmaps := []revokeMap{
		{
			since: time.Now(),
			kvs:   make(map[string]revokeSet),
		},
	}
	revoker.revokeMaps.Store(&rmaps)

	go revoker.revokeRotator()
	return revoker
}

func (r *Revoker) revokeRotator() {
	ticker := time.NewTicker(r.revokeRotateInterval)

	for range ticker.C {
		r.rotate()
	}
}

func (r *Revoker) rotate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	old := r.revokeBuckets.Load()
	next := append([]revokeBucket{{since: time.Now(), trie: trie.NewTrie[revokeSet]()}}, (*old)...)
	if len(next) > r.revokeKeepBuckets {
		next = next[:r.revokeKeepBuckets]
	}
	r.revokeBuckets.Store(&next)
	oldMaps := r.revokeMaps.Load()
	nextMaps := append([]revokeMap{{since: time.Now(), kvs: make(map[string]revokeSet)}}, (*oldMaps)...)
	if len(nextMaps) > r.revokeKeepBuckets {
		nextMaps = nextMaps[:r.revokeKeepBuckets]
	}
	r.revokeMaps.Store(&nextMaps)
}

func (r *Revoker) current() *revokeBucket {
	buckets := r.revokeBuckets.Load()
	return &(*buckets)[0]
}

func (r *Revoker) Revoke(req *allino.Runtime, scope string, t time.Time, reason string) {
	idx := strings.Index(scope, "*")
	if idx < 0 {
		RevokeHandler.Call(req, &RevokeInput{
			Scope:  scope,
			Time:   t,
			Reason: reason,
		})
		return
	}

	RevokePrefixHandler.Call(req, &RevokeInput{
		Scope:  scope[:idx],
		Time:   t,
		Reason: reason,
	})
}

func (r *Revoker) Insert(scope string, t time.Time, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	kvs := r.revokeMaps.Load()
	kvss := &(*kvs)[0]
	if old, ok := kvss.kvs[scope]; ok && old.when > t.Unix() {
		return
	}
	kvss.kvs[scope] = revokeSet{
		when:   t.Unix(),
		reason: reason,
	}
}

func (r *Revoker) InsertPrefix(scope string, t time.Time, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.current().trie.Insert(scope, revokeSet{
		when:   t.Unix(),
		reason: reason,
	})
}

func (r *Revoker) IsRevoked(scope string, issuedAt time.Time) (revoked bool, reason string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	revoked, reason = r.isRevoked(scope, issuedAt)
	if revoked {
		return true, reason
	}

	return r.isRevokedPrefix(scope, issuedAt)
}

func (r *Revoker) isRevoked(scope string, issuedAt time.Time) (revoked bool, reason string) {
	for _, b := range *r.revokeMaps.Load() {
		t, ok := b.kvs[scope]
		if ok && !issuedAt.After(time.Unix(t.when, 0)) {
			return true, t.reason
		}
	}
	return false, ""
}

func (r *Revoker) isRevokedPrefix(scope string, issuedAt time.Time) (revoked bool, reason string) {
	for _, b := range *r.revokeBuckets.Load() {
		if b.trie.MatchAll(scope, func(value revokeSet, matchedPrefix string) bool {
			if !issuedAt.After(time.Unix(value.when, 0)) {
				revoked = true
				reason = value.reason
				return true
			}
			return false
		}) {
			return
		}
	}
	return
}

func Revoke(r *allino.Runtime, scope string, reason string) {
	revoker.Revoke(r, scope, time.Now(), reason)
}

func IsRevoked(r *allino.Runtime, scope string, issuedAt time.Time) (revoked bool, reason string) {
	return revoker.IsRevoked(scope, issuedAt)
}
