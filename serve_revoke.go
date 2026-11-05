package allino

type RevokeConfig struct {
	// UseSQLCookieRevoke enables synchronous per-cookie revocation in SQLite.
	// Requires the ext/revoker extension and a shared signing key across instances.
	UseSQLCookieRevoke bool `json:"use_sql_cookie_revoke"`
	UseLoginRevoke     bool `json:"use_login_revoke"`
}
