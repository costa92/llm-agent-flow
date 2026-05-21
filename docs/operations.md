# Operations — deploying `cmd/flowd`

How to run `flowd` in production. For first-time use see
[`tutorial.md`](tutorial.md); for what the v0.1.x freeze covers see
[`compatibility.md`](compatibility.md).

## Flags + env

```
flowd \
  --addr               :7861                    HTTP listen address.
  --db                 /var/lib/flowd/flow.db   SQLite DSN (":memory:" for ephemeral).
  --flow               <file.json>              Optional boot-time seed + legacy /run aliases.
  --tools              <manifest.json>          Tool catalog (http/exec kinds).
  --token              <secret>                 Or FLOWD_TOKEN env var.
  --read-timeout       5s                       HTTP server read timeout.
  --write-timeout      0                        HTTP write timeout (0 disables — required for SSE).
  --max-node-concurrency 0                      Per-layer goroutine cap (0 = unlimited).
```

Authentication, the database path, and `--write-timeout 0` are the
three settings real deployments care about most.

## Persistence layout

`--db` accepts any DSN `modernc.org/sqlite` understands. Practical
values:

| DSN | Use case |
|---|---|
| `:memory:` | Demos, tests, fully ephemeral runs (history dies with the process) |
| `/var/lib/flowd/flow.db` | Single-process on-disk; survives restarts |
| `file:/var/lib/flowd/flow.db?cache=shared&_pragma=journal_mode(WAL)` | WAL mode for concurrent readers |

The schema is created on `Open` and is idempotent. Backups are
plain `sqlite3 .backup` invocations against the file.

**Important:** SQLite is single-writer by design. `flowd` does not
shard across processes — run one instance per database file. For
multi-replica HA, share the DB at the storage layer (NFS / EBS) and
gate with a `flock` / lease, OR build a custom `flow/store.Store`
implementation backed by Postgres and feed it via `server.Config.Store`.

### Backup + restore

```bash
# Live backup (safe while flowd is running, WAL mode).
sqlite3 /var/lib/flowd/flow.db ".backup /backup/flow-$(date +%F).db"

# Restore: stop flowd, copy file in place, start.
systemctl stop flowd
cp /backup/flow-2026-05-21.db /var/lib/flowd/flow.db
systemctl start flowd
```

`run_events` grows linearly with workload. For a busy service,
consider a nightly prune:

```sql
DELETE FROM run_events
 WHERE run_id IN (
   SELECT id FROM runs WHERE started_at < ?  -- old runs cutoff
 );
DELETE FROM runs WHERE started_at < ?;
VACUUM;
```

There's no built-in retention flag in v0.1.x — it's an explicit
non-goal; ops teams have site-specific policies. The schema is
documented in
[`flow/store/sqlite/open.go`](../flow/store/sqlite/open.go).

## Auth

Disabled by default. Enable with `--token` (or `FLOWD_TOKEN`):

```bash
flowd --addr :7861 --db /var/lib/flowd/flow.db \
      --token "$(cat /etc/flowd/token)"
```

When set, every endpoint **except `/healthz`** requires:

```
Authorization: Bearer <secret>
```

Missing header → 401 + `WWW-Authenticate: Bearer realm="flowd"`.
Wrong token → 403. Bad scheme → 401.

Constant-time comparison; the bundled implementation in
`server.BearerTokenAuthenticator` is suitable for static-secret
shops. For richer auth (JWT, mTLS, OAuth introspection, per-user
audit), wire a custom `server.Authenticator`:

```go
type myAuth struct { /* ... */ }
func (a *myAuth) Authenticate(r *http.Request) error {
    // ... return server.ErrUnauthorized for 401, any other err for 403
    return nil
}

srv, _ := server.New(server.Config{
    // ... usual fields ...
    Authenticator: &myAuth{},
})
```

The `/healthz` bypass is hardcoded — k8s liveness probes and load
balancers don't need credentials.

**At v0.1 there is no rate limiting and no audit log.** Both are
explicit non-goals at this band; layer them via an upstream API
gateway (nginx, envoy, kong, etc.).

## OpenTelemetry

`cmd/flowd` does not configure an OTel pipeline itself. To get
tracing, run with a TracerProvider configured at the SDK level:

```go
// In a custom main wired around server.New(cfg):
import (
    "go.opentelemetry.io/otel"
    "github.com/costa92/llm-agent-otel" // root pkg sets up SDK exporters
)
// ... wire TracerProvider via otel.SetTracerProvider(tp) BEFORE
//     server.New(cfg). The flowd server itself emits no spans;
//     spans come from otelflow.Wrap() called by user code.
```

In practice the recommended pattern is: **embed `cmd/flowd/server`
in your own binary** that ALSO does OTel setup and wraps the engine
with `otelflow.Wrap` before passing it. Without that wrapper, flowd
runs unanned — the engine cache stores plain `*flow.Engine`s, not
traced runners.

A future flowd release may invert this: take a `flow.Runner`
factory in Config so any engine produced for a flow id can be
auto-wrapped. Until then, plain `flowd` traces nothing.

If you only want **otel-exporter envelope** logging (errors,
warnings, requests), tap stdout — the embedded `log.Default()`
emits per-request error lines via `cfg.Logger`.

## Performance tuning

### `--max-node-concurrency`

Caps the number of goroutines a single topological layer may spawn
in parallel. Default `0` = unlimited (one goroutine per node).
Lower values throttle aggressively when individual nodes are
expensive (LLM calls); 0 / unlimited is the right default when
nodes are cheap (string ops, local subprocesses).

### `EngineCacheSize`

The compiled-engine cache is keyed by flow id. Bound it via
`server.Config.EngineCacheSize` to prevent unbounded growth in
deployments with many distinct flows:

```go
srv, _ := server.New(server.Config{
    // ...
    EngineCacheSize: 256, // capacity-bounded LRU
})
```

A non-positive value (the default) disables bounding — the cache
grows indefinitely. PUT / DELETE handlers always evict explicitly
regardless of cap.

### Event-INSERT batching (automatic for sync runs)

`POST /flows/{id}/run` (sync mode) buffers events in memory and
flushes them in a **single transaction** at `FinishRun` time —
roughly N× faster than per-event INSERTs for a flow with N events.

`POST /flows/{id}/run/stream` still persists events **before**
forwarding each frame, so a client that drops mid-stream leaves a
complete audit trail. Operators trade a small latency cost for
durability on stream runs.

### When to scale horizontally

A single `flowd` process handles many hundreds of concurrent runs
on modern hardware. Bottlenecks tend to surface in this order:

1. **Tool latency** (HTTP/exec) — dominates wall-time. Cache or
   batch upstream of the tool. Use `--max-node-concurrency` to
   limit fanout against rate-limited tool services.
2. **SQLite writes** — write-throughput plateau ~2-5 K
   transactions/sec on local disk. Enable WAL mode and increase
   page cache via the DSN if you need more.
3. **Engine cache misses** — pure-CPU. Raise `EngineCacheSize`.

For horizontal scale, see "Persistence layout" above: SQLite is
single-writer, so the practical path is one flowd per DB and
upstream routing.

## Graceful shutdown

`flowd` traps `SIGINT` + `SIGTERM` and runs `http.Server.Shutdown`
with a 5-second deadline. In-flight runs complete; new connections
are refused. The SQLite handle closes on the deferred `store.Close()`.

For zero-downtime deploys behind a load balancer: drain at the LB,
wait for `/healthz` to stop being routed to the old replica, then
SIGTERM. The default 5 s deadline assumes runs are short; for
long-running flows raise it via patching the source or front this
binary with `runit`/`s6` style supervisors that handle deeper
deadlines.

## Observability quick checks

```bash
# Liveness — does the process respond?
curl -fsSL http://localhost:7861/healthz
# ok

# Did flows persist across restart?
curl -fsSL http://localhost:7861/flows | jq '.flows | length'

# How busy was the last hour?  (no auth)
sqlite3 flow.db "
  SELECT status, COUNT(*) FROM runs
   WHERE started_at > (strftime('%s', 'now') - 3600) * 1000000
   GROUP BY status
"
```

## Upgrade path

`v0.1.x` is additive-only. Bumps within the v0.1 line are
drop-in — no flag changes, no DB migration, no client recompile
required.

`v0.2.0` (when it ships) WILL be allowed to break the IR / API
shape; clear migration notes will appear in [CHANGELOG](../CHANGELOG.md)
and a `/v2` module path will replace `/v0.1` for the breaking parts.

## Non-goals at v0.1

Documented here so operators don't expect them:

- No rate limiting (use upstream gateway).
- No audit log beyond per-run events (use OTel + log shipping).
- No multi-tenancy (one DB per tenant).
- No clustering (single-writer SQLite — federate at the storage tier).
- No replay-on-restart of in-flight runs (FinishRun on shutdown is
  abrupt; runs in `running` state stay that way after a crash and
  must be cleaned up manually).
- No flow scheduling / cron triggers (build above the API).
