package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	"github.com/redis/go-redis/v9"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

// Claim-based durable dequeue for the sorted-set transport.
//
// Requests leave the pending zset only when claimed under a lease; the claim
// is dropped exactly once per terminal outcome (ack, release, or expiry
// redelivery). Delivery is at-least-once across crashes; a per-request
// marker collapses duplicate results to exactly one record. Requires Redis
// persistence (AOF/replication). See docs/guides/durable-dequeue.md.

const (
	// reclaimGraceAfterDeadline extends leases slightly past the request
	// deadline so a claim never expires into an already-expired request; the
	// deadline-exceeded path then terminates it deterministically.
	reclaimGraceAfterDeadline = 30 * time.Second

	// reclaimBatchSize bounds how many expired claims one reclaimer pass
	// releases, keeping each tick's Redis work bounded under backlog.
	reclaimBatchSize = 100

	// claimTokenBytes is the entropy of the per-claim ownership token.
	claimTokenBytes = 8
)

// claimKeys bundles the Redis keys implementing claims for one queue.
type claimKeys struct {
	pending string // zset: reqID-scored members awaiting dispatch
	claimed string // hash: reqID -> original member JSON
	owners  string // hash: reqID -> ownership token
	idx     string // zset: reqID -> lease expiry unix seconds
}

func newClaimKeys(queueName string) claimKeys {
	return claimKeys{
		pending: queueName,
		claimed: queueName + ":claimed",
		owners:  queueName + ":claim-owners",
		idx:     queueName + ":claims-idx",
	}
}

// terminalKey returns the dedup marker guarding against duplicate result
// records when at-least-once redelivery lets two owners finish one request.
func terminalKey(requestID string) string {
	return "result-terminal:" + requestID
}

// CLAIM moves one member from pending to claimed. Returns 0 when another
// consumer won. Overwrites an existing self-claim (retried requests re-enter
// pending while still owned). Stores the original sort score under an
// "<id>:score" companion field for exact restoration.
var claimScript = redis.NewScript(`
if redis.call('ZREM', KEYS[1], ARGV[2]) == 0 then
  return 0
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[2], ARGV[1] .. ':score', ARGV[5])
redis.call('HSET', KEYS[3], ARGV[1], ARGV[3])
redis.call('ZADD', KEYS[4], ARGV[4], ARGV[1])
return 1
`)

// RELEASE hands a claimed request back to pending at its original sort score.
// Token-guarded: a stale owner must not drop the new owner's claim.
var releaseClaimScript = redis.NewScript(`
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[4] then
  return 0
end
local score = redis.call('HGET', KEYS[2], ARGV[1] .. ':score')
if not score then
  score = ARGV[3]
end
redis.call('ZADD', KEYS[1], tonumber(score), ARGV[2])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1] .. ':score')
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
return 1
`)

// ACKRESULT records a terminal result and drops the claim atomically; the
// dedup marker makes the record idempotent (first ack pushes, rest clean up).
//
// KEYS: marker, resultList, claimed, owners, idx
// ARGV: id, resultJSON, markerTTLSeconds, listTTLSeconds, token
// Returns 1 when the result was recorded, 0 when suppressed as a duplicate.
var ackResultScript = redis.NewScript(`
local pushed = 0
if redis.call('EXISTS', KEYS[1]) == 0 then
  redis.call('LPUSH', KEYS[2], ARGV[2])
  local listTTL = tonumber(ARGV[4])
  if listTTL > 0 then
    redis.call('EXPIRE', KEYS[2], listTTL)
  end
  local markerTTL = tonumber(ARGV[3])
  if markerTTL > 0 then
    redis.call('SETEX', KEYS[1], markerTTL, '1')
  else
    -- A zero/negative TTL means "unbounded"; plain SET avoids the invalid
    -- expire error that SETEX raises for 0.
    redis.call('SET', KEYS[1], '1')
  end
  pushed = 1
end
if redis.call('HGET', KEYS[4], ARGV[1]) == ARGV[5] then
  redis.call('HDEL', KEYS[3], ARGV[1])
  redis.call('HDEL', KEYS[3], ARGV[1] .. ':score')
  redis.call('HDEL', KEYS[4], ARGV[1])
  redis.call('ZREM', KEYS[5], ARGV[1])
end
return pushed
`)

// RECLAIMIFEXPIRED redelivers an expired claim back to pending at its
// original sort score; renewed claims and ghosts are left alone.
var reclaimExpiredScript = redis.NewScript(`
local exp = redis.call('ZSCORE', KEYS[4], ARGV[1])
if not exp then
  return 0
end
if tonumber(exp) > tonumber(ARGV[3]) then
  return 0
end
local payload = redis.call('HGET', KEYS[2], ARGV[1])
if not payload then
  redis.call('ZREM', KEYS[4], ARGV[1])
  return 0
end
local score = redis.call('HGET', KEYS[2], ARGV[1] .. ':score')
if not score then
  score = ARGV[2]
end
redis.call('ZADD', KEYS[1], tonumber(score), payload)
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1] .. ':score')
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
return 1
`)

// RENEWCLAIM extends a live claim's lease (retry flusher). Not token-guarded;
// worst case briefly extends a foreign lease, delaying reclaim by one TTL.
var renewClaimScript = redis.NewScript(`
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 0 then
  return 0
end
redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[1])
return 1
`)

// newClaimToken generates the per-claim ownership token that lets ack/release
// distinguish "my claim" from "a claim redelivered to another instance".
func newClaimToken() (string, error) {
	buf := make([]byte, claimTokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate claim token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// claimHandle is what this instance tracks per in-flight request: the
// ownership proof plus everything the heartbeater and shutdown sweep need to
// renew or hand back the claim without re-reading Redis state.
type claimHandle struct {
	token    string
	queue    string
	deadline float64
}

// claimExpiry computes the lease deadline for a request claimed now: the
// lease TTL, capped so it never outlives the request deadline plus grace —
// an expired request must terminate through the deadline-exceeded path, not
// linger claimed.
func (r *RedisSortedSetFlow) claimExpiry(deadline float64) float64 {
	now := float64(time.Now().Unix())
	expiry := now + r.claimLeaseTTL.Seconds()
	if grace := float64(reclaimGraceAfterDeadline.Seconds()); deadline > 0 && deadline+grace < expiry {
		expiry = deadline + grace
	}
	return expiry
}

// claimRequest claims one peeked request. On success the caller owns the
// request until it acks a terminal result, releases it, or dies. originalScore
// is the pending zset score it was peeked at; release/redelivery restores it.
func (r *RedisSortedSetFlow) claimRequest(ctx context.Context, queueName string, ir *api.InternalRequest, member string, deadline float64, originalScore float64) (token string, ok bool, err error) {
	token, err = newClaimToken()
	if err != nil {
		return "", false, err
	}
	keys := newClaimKeys(queueName)
	res, err := claimScript.Run(ctx, r.rdb, []string{
		keys.pending, keys.claimed, keys.owners, keys.idx,
	}, ir.PublicRequest.ReqID(), member, token, r.claimExpiry(deadline), originalScore).Int()
	if err != nil {
		return "", false, fmt.Errorf("claim request %q on queue %q: %w", ir.PublicRequest.ReqID(), queueName, err)
	}
	if res == 0 {
		return "", false, nil
	}
	r.claimTokens.Store(ir.PublicRequest.ReqID(), &claimHandle{
		token:    token,
		queue:    queueName,
		deadline: deadline,
	})
	return token, true, nil
}

// releaseClaim returns a claimed request to pending during graceful shutdown.
func (r *RedisSortedSetFlow) releaseClaim(ctx context.Context, queueName string, requestID string, member string, deadline float64, token string) error {
	keys := newClaimKeys(queueName)
	err := releaseClaimScript.Run(ctx, r.rdb, []string{
		keys.pending, keys.claimed, keys.owners, keys.idx,
	}, requestID, member, deadline, token).Err()
	if err != nil {
		return fmt.Errorf("release claim for %q on queue %q: %w", requestID, queueName, err)
	}
	r.claimTokens.Delete(requestID)
	r.retryOwned.Delete(requestID)
	return nil
}

// ackResult records a terminal result (idempotently) and drops this flow's
// claim. claimQueueName hosts the claim bookkeeping; resultList is the
// resolved destination. pushed=false means a duplicate was suppressed.
func (r *RedisSortedSetFlow) ackResult(ctx context.Context, claimQueueName string, resultList string, requestID string, resultJSON string, listTTL time.Duration) (pushed bool, err error) {
	// Peek the token rather than consuming it: if the script errors the
	// caller may retry this ack, and the ownership proof must survive.
	var token string
	if v, ok := r.claimTokens.Load(requestID); ok {
		if h, ok := v.(*claimHandle); ok {
			token = h.token
		}
	}
	keys := newClaimKeys(claimQueueName)
	markerTTL := int64(r.resultDedupTTL.Seconds())
	listTTLSec := int64(0)
	if listTTL > 0 {
		listTTLSec = int64(listTTL.Seconds())
	}
	res, err := ackResultScript.Run(ctx, r.rdb, []string{
		terminalKey(requestID), resultList, keys.claimed, keys.owners, keys.idx,
	}, requestID, resultJSON, markerTTL, listTTLSec, token).Int()
	if err != nil {
		return false, fmt.Errorf("ack result for %q: %w", requestID, err)
	}
	r.claimTokens.Delete(requestID)
	r.retryOwned.Delete(requestID)
	if res == 0 {
		metrics.RecordDuplicateSuppressed()
		return false, nil
	}
	return true, nil
}

// renewClaim extends the lease of a request being sent to retry. Ownership is
// retained across the backoff; the eventual terminal result acks and releases.
func (r *RedisSortedSetFlow) renewClaim(ctx context.Context, queueName string, requestID string, deadline float64) error {
	keys := newClaimKeys(queueName)
	err := renewClaimScript.Run(ctx, r.rdb, []string{keys.claimed, keys.idx},
		requestID, r.claimExpiry(deadline)).Err()
	if err != nil {
		return fmt.Errorf("renew claim for %q on queue %q: %w", requestID, queueName, err)
	}
	return nil
}

// reclaimExpiredClaims releases every claim whose lease has lapsed, returning
// each request to its queue's pending zset for redelivery. Runs on whichever
// instance holds the transport open; instances that died cannot run it, which
// is precisely the point — survivors take over their in-flight work.
func (r *RedisSortedSetFlow) reclaimExpiredClaims(ctx context.Context) (released int, err error) {
	logger := log.FromContext(ctx)
	now := float64(time.Now().Unix())

	for _, ch := range r.requestChannels {
		queueName := ch.queueName
		keys := newClaimKeys(queueName)
		expiredIDs, err := r.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
			Key: keys.idx, ByScore: true,
			Start: "-inf", Stop: fmt.Sprintf("%f", now),
			Count: reclaimBatchSize, Offset: 0,
		}).Result()
		if err != nil {
			return released, fmt.Errorf("read expired claims on queue %q: %w", queueName, err)
		}
		for _, id := range expiredIDs {
			payload, err := r.rdb.HGet(ctx, keys.claimed, id).Result()
			if err != nil && err != redis.Nil {
				return released, fmt.Errorf("read claim payload for %q: %w", id, err)
			}
			deadline := float64(0)
			if err == nil {
				var ir api.InternalRequest
				if jsonErr := json.Unmarshal([]byte(payload), &ir); jsonErr == nil && ir.PublicRequest != nil {
					deadline = float64(ir.PublicRequest.ReqDeadline())
				}
			}
			res, err := reclaimExpiredScript.Run(ctx, r.rdb, []string{
				keys.pending, keys.claimed, keys.owners, keys.idx,
			}, id, deadline, now).Int()
			if err != nil {
				return released, fmt.Errorf("reclaim claim for %q on queue %q: %w", id, queueName, err)
			}
			if res == 1 {
				released++
				metrics.RecordClaimExpired(ch.queueID, queueName, r.poolNameFor(ch.queueID))
				logger.V(logutil.DEBUG).Info("Reclaimed expired claim, redelivering", "id", id, "queue", queueName)
			}
		}
		if depth, err := r.rdb.HLen(ctx, keys.claimed).Result(); err == nil {
			metrics.SetClaimDepth(float64(depth), ch.queueID, queueName, r.poolNameFor(ch.queueID))
		}
	}
	return released, nil
}

// startReclaimer launches the background loop that redelivers lapsed claims.
// It runs on the drain context so it keeps working through the graceful
// shutdown window, catching claims abandoned by workers that hit DrainTimeout.
func (r *RedisSortedSetFlow) startReclaimer(ctx context.Context) {
	logger := log.FromContext(ctx)
	ticker := time.NewTicker(r.claimReclaimInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.reclaimExpiredClaims(ctx); err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to reclaim expired claims")
			}
		}
	}
}

// SWEEPHANDBACK returns a still-claimed request to pending at shutdown,
// reading payload/score from the bookkeeping itself. Token-guarded.
var sweepClaimScript = redis.NewScript(`
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then
  return 0
end
local payload = redis.call('HGET', KEYS[2], ARGV[1])
local score = redis.call('HGET', KEYS[2], ARGV[1] .. ':score')
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1] .. ':score')
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
if payload then
  local sc = score
  if not sc then
    sc = ARGV[3]
  end
  redis.call('ZADD', KEYS[1], tonumber(sc), payload)
  return 1
end
return 0
`)

// heartbeatInterval is how often live claims are renewed: a third of the
// lease TTL, clamped so ticks are neither sub-second spam nor multi-minute
// gaps.
func (r *RedisSortedSetFlow) heartbeatInterval() time.Duration {
	hb := r.claimLeaseTTL / 3
	if hb < time.Second {
		hb = time.Second
	}
	if hb > 30*time.Second {
		hb = 30 * time.Second
	}
	return hb
}

// heartbeatClaims renews every held claim so slow-but-alive work is not
// treated as dead. Acked/released ids leave the map and are skipped.
func (r *RedisSortedSetFlow) heartbeatClaims(ctx context.Context) {
	logger := log.FromContext(ctx)
	r.claimTokens.Range(func(key, value any) bool {
		id, _ := key.(string)
		h, ok := value.(*claimHandle)
		if !ok || h == nil || id == "" {
			return true
		}
		if err := r.renewClaim(ctx, h.queue, id, h.deadline); err != nil {
			logger.V(logutil.DEBUG).Error(err, "Failed to renew claim lease", "id", id)
		}
		return true
	})
}

// startHeartbeat renews held claims until the flow's drain context closes.
// It runs alongside the reclaimer: the reclaimer catches owners that died,
// the heartbeater proves this owner is not one of them.
func (r *RedisSortedSetFlow) startHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(r.heartbeatInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.heartbeatClaims(ctx)
		}
	}
}

// sweepUnackedClaims hands still-claimed requests back to pending at
// shutdown (except retryOwned ones) instead of letting them wait out their
// lease. Returns how many claims were swept.
func (r *RedisSortedSetFlow) sweepUnackedClaims(ctx context.Context) int {
	logger := log.FromContext(ctx)
	swept := 0
	r.claimTokens.Range(func(key, value any) bool {
		id, _ := key.(string)
		h, ok := value.(*claimHandle)
		if !ok || h == nil || id == "" {
			return true
		}
		if _, retried := r.retryOwned.Load(id); retried {
			return true
		}
		keys := newClaimKeys(h.queue)
		err := retryRedisOp(ctx, func(ctx context.Context) error {
			return sweepClaimScript.Run(ctx, r.rdb, []string{
				keys.pending, keys.claimed, keys.owners, keys.idx,
			}, id, h.token, h.deadline).Err()
		})
		if err != nil {
			logger.Error(err, "Failed to sweep claim on shutdown", "id", id, "queue", h.queue)
			return true
		}
		r.claimTokens.Delete(id)
		swept++
		return true
	})
	return swept
}
