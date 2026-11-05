package revoker

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRevokerConcurrentAccess(t *testing.T) {
	r := NewRevoker()
	now := time.Now().Truncate(time.Second)
	var wg sync.WaitGroup
	for n := 0; n < 16; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				scope := fmt.Sprintf("user-%d-%d", n, j)
				r.Insert(scope, now, "logout")
				if yes, _ := r.IsRevoked(scope, now); !yes {
					t.Errorf("missing revocation")
				}
				r.InsertPrefix(scope+"/", now, "logout")
				if yes, _ := r.IsRevoked(scope+"/child", now); !yes {
					t.Errorf("missing prefix revocation")
				}
			}
		}(n)
	}
	wg.Wait()
	if yes, _ := r.IsRevoked("user-0-0", now.Add(time.Second)); yes {
		t.Fatal("new login revoked")
	}
}

func TestRevokerRotationAndOrdering(t *testing.T) {
	r := NewRevoker()
	now := time.Now().Truncate(time.Second)
	r.Insert("owner", now, "new")
	r.Insert("owner", now.Add(-time.Second), "old")
	if yes, reason := r.IsRevoked("owner", now); !yes || reason != "new" {
		t.Fatal("out-of-order delivery undid revocation")
	}
	r.InsertPrefix("team/", now, "logout")
	r.rotate()
	if yes, _ := r.IsRevoked("owner", now); !yes {
		t.Fatal("lost exact revocation on rotation")
	}
	if yes, _ := r.IsRevoked("team/member", now); !yes {
		t.Fatal("lost prefix revocation on rotation")
	}
	for n := 0; n < r.revokeKeepBuckets; n++ {
		r.rotate()
	}
	if yes, _ := r.IsRevoked("owner", now); yes {
		t.Fatal("exact buckets never expired")
	}
}
