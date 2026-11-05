package revoker

import (
	"database/sql"
	"errors"
	"time"

	"github.com/wh-kuromai/allino"
	"github.com/wh-kuromai/cryptino"
)

// SQLite cookie revocation is synchronous, persists across restarts, and does not
// require Redis fanout. All instances must share the same database and key.
func initializeSQLCookies(s *allino.Server, _ *allino.Runtime) error {
	if !s.Config.Login.Revoke.UseSQLCookieRevoke {
		return nil
	}
	if s.Config.SQL.Driver != "sqlite" && s.Config.SQL.Driver != "sqlite3" {
		return errors.New("SQL cookie revocation currently requires SQLite")
	}
	if s.SQL == nil {
		return errors.New("SQL cookie revocation requires SQL")
	}
	_, err := s.SQL.Exec(`CREATE TABLE IF NOT EXISTS allino_revoked_cookies (
 jti TEXT PRIMARY KEY, expires_at BIGINT NOT NULL
 ); CREATE INDEX IF NOT EXISTS allino_revoked_cookies_expiry ON allino_revoked_cookies(expires_at);`)
	return err
}

func checkSQLCookie(r *allino.Runtime, jwt *cryptino.JSONWebToken) error {
	if jwt.Body.Audience != r.Config().Login.LoginCookie.JWTAudience {
		return nil
	}
	if jwt.Body.JwtID == "" || jwt.Body.ExpirationTime <= time.Now().Unix() {
		return allino.ErrNotLoggedIn
	}
	var found string
	err := r.SQL().QueryRowContext(r.Context(), "SELECT jti FROM allino_revoked_cookies WHERE jti=?", jwt.Body.JwtID).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return allino.ErrNotLoggedIn
}

// RevokeLoginCookie invalidates only the current login cookie, including copies
// held by other clients. Invalid or expired cookies are already unusable.
func RevokeLoginCookie(r *allino.Runtime) error {
	if !r.Config().Login.Revoke.UseSQLCookieRevoke {
		return errors.New("SQL cookie revocation is disabled")
	}
	token := r.Fiber().Cookies(r.Config().Login.LoginCookie.Name)
	if token == "" {
		return nil
	}
	jwt, err := cryptino.VerifyJWT(cryptino.ES256(), []byte(token), r.Config().Login.PublicKey)
	if err != nil || jwt.Body.Audience != r.Config().Login.LoginCookie.JWTAudience || jwt.Body.ExpirationTime <= time.Now().Unix() || jwt.Body.JwtID == "" {
		return nil
	}
	tx, err := r.SQL().BeginTx(r.Context(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(r.Context(), "DELETE FROM allino_revoked_cookies WHERE expires_at<=?", time.Now().Unix()); err != nil {
		return err
	}
	if _, err = tx.ExecContext(r.Context(), "INSERT INTO allino_revoked_cookies(jti,expires_at) VALUES(?,?) ON CONFLICT(jti) DO NOTHING", jwt.Body.JwtID, jwt.Body.ExpirationTime); err != nil {
		return err
	}
	return tx.Commit()
}
