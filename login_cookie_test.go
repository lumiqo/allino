package allino

import (
	"testing"
	"time"
)

func TestLoginJWTNumericDates(t *testing.T) {
	before := time.Now().Unix()
	jwt := loginJWT("owner", 12*time.Hour)
	after := time.Now().Unix()
	if jwt.Body.IssuedAt < before || jwt.Body.IssuedAt > after {
		t.Fatal("invalid issued-at")
	}
	if jwt.Body.ExpirationTime < before+43200 || jwt.Body.ExpirationTime > after+43200 {
		t.Fatal("expiry must use seconds")
	}
	if jwt.Body.JwtID == "" || jwt.Body.JwtID == loginJWT("owner", 12*time.Hour).Body.JwtID {
		t.Fatal("missing unique token ID")
	}
}
