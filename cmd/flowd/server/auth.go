package server

import (
	"errors"
	"net/http"
	"strings"
)

// Authenticator decides whether a request is allowed through. The
// default BearerTokenAuthenticator compares "Authorization: Bearer X"
// against a configured static token; callers wire their own
// implementation (JWT, mTLS, IP allowlist, etc.) by setting
// Config.Authenticator.
//
// Authenticate returns nil to allow the request. Returning
// ErrUnauthorized makes the middleware respond 401; any other
// non-nil error responds 403. The middleware never reads or modifies
// the request body.
type Authenticator interface {
	Authenticate(r *http.Request) error
}

// ErrUnauthorized signals "missing or malformed credentials".
// Returning it from Authenticate produces 401. Any other non-nil
// error produces 403.
var ErrUnauthorized = errors.New("unauthorized")

// BearerTokenAuthenticator authenticates "Authorization: Bearer X"
// against a static token. An empty Token disables authentication —
// the middleware short-circuits and every request is allowed, which
// is the default for backward compatibility with v0.0.7 callers.
type BearerTokenAuthenticator struct {
	Token string
}

// Authenticate implements Authenticator.
func (a BearerTokenAuthenticator) Authenticate(r *http.Request) error {
	if a.Token == "" {
		return nil
	}
	header := r.Header.Get("Authorization")
	if header == "" {
		return ErrUnauthorized
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return ErrUnauthorized
	}
	got := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	if got == "" {
		return ErrUnauthorized
	}
	// Constant-time-ish comparison to avoid leaking timing info on a
	// shared secret. crypto/subtle would be ideal; for v0.0.x a plain
	// equality with a length-prefix gate is acceptable.
	if len(got) != len(a.Token) {
		return errors.New("forbidden")
	}
	var diff byte
	for i := 0; i < len(got); i++ {
		diff |= got[i] ^ a.Token[i]
	}
	if diff != 0 {
		return errors.New("forbidden")
	}
	return nil
}

// authBypass returns true for paths that the middleware lets through
// regardless of Authenticator. /healthz must stay open so external
// monitors (k8s liveness, load balancer health checks) work without
// a token.
func authBypass(path string) bool {
	return path == "/healthz"
}

// withAuth wraps next with the configured authenticator. When the
// authenticator is nil the wrapper is a pass-through, preserving the
// v0.0.7 behavior.
func withAuth(auth Authenticator, next http.Handler) http.Handler {
	if auth == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if authBypass(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		if err := auth.Authenticate(r); err != nil {
			status := http.StatusForbidden
			if errors.Is(err, ErrUnauthorized) {
				status = http.StatusUnauthorized
				w.Header().Set("WWW-Authenticate", `Bearer realm="flowd"`)
			}
			writeError(w, status, err)
			return
		}
		next.ServeHTTP(w, r)
	})
}
