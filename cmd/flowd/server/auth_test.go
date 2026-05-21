package server_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/costa92/llm-agent-flow/cmd/flowd/server"
)

func withAuth(tok string) serverOption {
	return func(cfg *server.Config) {
		cfg.Authenticator = server.BearerTokenAuthenticator{Token: tok}
	}
}

func TestAuthDisabledByDefault(t *testing.T) {
	srv, _ := newStoreServer(t)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/flows")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 with auth disabled", resp.StatusCode)
	}
}

func TestAuthHealthzAlwaysBypass(t *testing.T) {
	srv, _ := newStoreServer(t, withAuth("sekret"))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("/healthz status = %d, want 200 (auth bypass)", resp.StatusCode)
	}
}

func TestAuthMissingHeaderIs401(t *testing.T) {
	srv, _ := newStoreServer(t, withAuth("sekret"))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/flows")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	if !strings.Contains(resp.Header.Get("WWW-Authenticate"), "Bearer") {
		t.Fatalf("WWW-Authenticate = %q, want Bearer challenge", resp.Header.Get("WWW-Authenticate"))
	}
}

func TestAuthWrongTokenIs403(t *testing.T) {
	srv, _ := newStoreServer(t, withAuth("sekret"))
	defer srv.Close()
	req, _ := http.NewRequest("GET", srv.URL+"/flows", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 403 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d (body=%s), want 403", resp.StatusCode, raw)
	}
}

func TestAuthMalformedHeaderIs401(t *testing.T) {
	srv, _ := newStoreServer(t, withAuth("sekret"))
	defer srv.Close()
	req, _ := http.NewRequest("GET", srv.URL+"/flows", nil)
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("status = %d, want 401 (not Bearer scheme)", resp.StatusCode)
	}
}

func TestAuthCorrectTokenAllowsRequest(t *testing.T) {
	srv, _ := newStoreServer(t, withAuth("sekret"))
	defer srv.Close()
	req, _ := http.NewRequest("GET", srv.URL+"/flows", nil)
	req.Header.Set("Authorization", "Bearer sekret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d (body=%s), want 200", resp.StatusCode, raw)
	}
}

func TestCustomAuthenticatorWorks(t *testing.T) {
	// A bespoke Authenticator that checks an X-Internal-Caller
	// header. Verifies the Authenticator interface is the right
	// extension point for non-token schemes.
	custom := server.Config{Authenticator: customAuth{}}
	srv, _ := newStoreServer(t, func(cfg *server.Config) {
		cfg.Authenticator = custom.Authenticator
	})
	defer srv.Close()

	// Missing header → 401.
	resp, _ := http.Get(srv.URL + "/flows")
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("missing header status = %d, want 401", resp.StatusCode)
	}
	// Correct header → 200.
	req, _ := http.NewRequest("GET", srv.URL+"/flows", nil)
	req.Header.Set("X-Internal-Caller", "trusted-svc")
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("correct header status = %d, want 200", resp.StatusCode)
	}
}

type customAuth struct{}

func (customAuth) Authenticate(r *http.Request) error {
	if r.Header.Get("X-Internal-Caller") == "trusted-svc" {
		return nil
	}
	return server.ErrUnauthorized
}

// Sanity: an Authenticator returning an unrelated error maps to 403.
func TestAuthGenericErrorIs403(t *testing.T) {
	srv, _ := newStoreServer(t, func(cfg *server.Config) {
		cfg.Authenticator = forbidAuth{}
	})
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/flows")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

type forbidAuth struct{}

func (forbidAuth) Authenticate(_ *http.Request) error { return errors.New("nope") }

// Make sure httptest.NewServer setup helper still passes its own
// auth headers through the existing test paths — we want to use the
// non-auth path when no token is configured.
var _ = httptest.NewServer
