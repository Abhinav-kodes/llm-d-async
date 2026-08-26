# Durable Dequeue (Claim / Lease / Ack)

Related:
- [Bug #404: Accepted Redis requests can be lost when an Async pod is hard-killed](https://github.com/llm-d/llm-d-async/issues/404)
- [batch-gateway #644: Async results can be lost with multiple Batch Processor replicas](https://github.com/llm-d/llm-d-batch-gateway/issues/644) (result-side counterpart)
- [batch-gateway #645: Resume in-progress batches after Processor pod or node loss](https://github.com/llm-d/llm-d-batch-gateway/issues/645)

## The problem

The redis-sortedset transport used to dequeue with `ZPOPMIN`: the request was
removed from Redis before any processing happened. Between that pop and the
result being pushed back to Redis, the request existed only in process memory.
Any hard stop in that window — SIGKILL, OOM, node loss — silently lost every
accepted request it held. Graceful shutdown covered only the requests still
sitting on a single channel send.

## The model

Dequeue is now **peek → claim → ack**:

1. **Peek** — each poll reads up to `batch_size` entries with `ZRANGEBYSCORE`
   (non-destructive). Nothing leaves the pending sorted set yet.
2. **Claim** — for each entry that passes deadline/cancellation/gate checks, a
   Lua script atomically moves it out of the pending set into claim
   bookkeeping:
   - `<queue>:claimed` — hash of the original member JSON (plus its original
     sort score under a `<id>:score` field),
   - `<queue>:claim-owners` — a random ownership token per claim,
   - `<queue>:claims-idx` — zset of claims scored by lease expiry
     (`min(claim_lease_ttl, deadline + 30s)`).
3. **Process as before** — claimed requests flow through the same channels,
   merge policy, and workers. No downstream change.
4. **Ack** — when a terminal result is flushed, one Lua script pushes the
   record to the result list, writes a `result-terminal:<id>` dedup marker,
   and drops the claim — atomically. A crash between "inference done" and
   "result written" therefore redelivers the request instead of losing it.

While a request is held, a background **heartbeater** renews its lease every
`claim_lease_ttl / 3` (clamped to 1s–30s), so slow-but-healthy inference is
never mistaken for a dead owner. The lease TTL is therefore the crash
*detection* window, not a processing-time budget.

Every exit path is paired with exactly one claim outcome:

| Path | Claim outcome |
|---|---|
| Result produced (success/error/cancelled/deadline/drop) | acked after the record is durably pushed |
| Request parked for retry | lease renewed; shielded from the shutdown sweep |
| Consumer context cancelled mid-hand-off | released back to pending at its original sort score |
| Graceful shutdown with unacked claims | swept back to pending immediately (except retry-owned) |
| Owner dies (lease expires) | reclaimer redelivers to pending; another instance picks it up |
| Gate refuses / gate error | no claim was taken; entry simply stays pending |

## Delivery guarantees

- **At-least-once execution**: a request whose owner crashed is re-run by the
  survivor. Expensive inference may execute twice across a failure.
- **Exactly-once terminal records**: the `result-terminal:<id>` marker makes
  duplicate results collapse — only the first ack pushes a record; later ones
  clean up their claim and no-op. Consumers observe one terminal record per
  accepted request, keyed by the internal request ID (`custom_id` is
  user-supplied and deliberately not used).
- Ordering within a queue remains earliest-deadline-first; release and
  redelivery restore the original sort score.

## Configuration

| Flag | Config JSON field | Default | Meaning |
|---|---|---|---|
| `--claim-lease-ttl` | `claim_lease_ttl_seconds` | `300` | Crash-detection window: how long a claim survives without a heartbeat before survivors redeliver the request. |
| `--claim-reclaim-interval` | `claim_reclaim_interval_ms` | `15000` | How often expired claims are scanned for redelivery. This bounds how long a crashed instance's work stalls. |
| `--result-dedup-ttl` | `result_dedup_ttl_seconds` | `21600` | Lifetime of per-request dedup markers; must exceed the longest possible redelivery chain. |

The heartbeat interval is derived (`lease TTL / 3`, clamped to 1s–30s) and is
not separately configurable. Flags apply to the redis-sortedset transport and
are ignored when `--transport-config`/`--transport-config-file` supplies its
own values.

Metrics: `async_claim_depth` (claimed per queue), `async_claims_expired_total`
(redeliveries — spikes indicate crashes or too-short leases),
`async_duplicate_results_suppressed_total` (duplicate records collapsed).

## Operational requirements

- **Redis persistence is part of the durability contract.** Claims, dedup
  markers, and queued requests all live in Redis; run it with AOF (`appendonly
  yes`, e.g. `appendfsync everysec`) and/or replication. Without persistence a
  Redis restart reintroduces a loss window this feature cannot close.
- Multiple Async replicas may share one queue: atomic claims prevent double
  dispatch, and lease expiry hands work over automatically when a replica
  disappears.
- Rolling upgrades are safe: pending members are unchanged, so old-version and
  new-version pods can interleave during rollout (entries popped by old pods
  do not get claim protection).

## Known limitations

- Heartbeats are not token-guarded: in the rare window after a lease lapse
  and takeover, the old owner's final heartbeat can extend the new owner's
  claim by at most one TTL, slightly delaying the next reclaim.
- Graceful shutdown hands unacked claims back to pending but cannot return
  requests held inside plugin goroutines that ignore context cancellation;
  those wait out their lease like hard-kill losses.
- No delivery counter or dead-letter queue: redelivery is bounded by each
  request's deadline — once it passes, the deadline-exceeded path produces the
  terminal record. Projects like asynq cap attempts and archive instead; here
  the deadline plays that role for batch workloads.
