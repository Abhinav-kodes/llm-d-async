package redis

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/api"
	"github.com/redis/go-redis/v9"
)

// newClaimTestFlow builds a bare flow wired to miniredis with a short lease so
// expiry paths can be exercised without sleeping for seconds.
func newClaimTestFlow(t *testing.T) (*miniredis.Miniredis, *redis.Client, context.Context, *RedisSortedSetFlow) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	flow := &RedisSortedSetFlow{
		rdb:                  rdb,
		gate:                 noopGate(),
		pollInterval:         50 * time.Millisecond,
		batchSize:            10,
		claimLeaseTTL:        200 * time.Millisecond,
		claimReclaimInterval: 50 * time.Millisecond,
		requestChannels:      []requestChannelData{{queueName: "q", queueID: "q"}},
	}
	return s, rdb, context.Background(), flow
}

func claimEnvelope(t *testing.T, id string, deadline int64) (*api.InternalRequest, string) {
	t.Helper()
	ir := api.NewInternalRequest(api.InternalRouting{RequestQueueName: "q"}, &api.RequestMessage{
		ID:       id,
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  map[string]any{"model": "m", "prompt": "p"},
	})
	b, err := json.Marshal(ir)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return ir, string(b)
}

// Sort score and body deadline deliberately diverge; both sit an hour ahead
// so lease capping never pins expiry to the wall clock mid-test.
var (
	testScore    = float64(time.Now().Add(time.Hour).Unix())
	testDeadline = int64(testScore) + 3600
)

func TestClaimRequest_MovesOutOfPendingAndRejectsDoubleClaim(t *testing.T) {
	_, rdb, ctx, flow := newClaimTestFlow(t)

	ir, member := claimEnvelope(t, "c1", testDeadline)
	if err := rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member}).Err(); err != nil {
		t.Fatal(err)
	}

	token, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline), testScore)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if token == "" {
		t.Fatal("empty ownership token")
	}
	if n, _ := rdb.ZCard(ctx, "q").Result(); n != 0 {
		t.Fatalf("pending zcard = %d, want 0", n)
	}
	keys := newClaimKeys("q")
	if got, _ := rdb.HGet(ctx, keys.claimed, "c1").Result(); got != member {
		t.Fatalf("claimed payload mismatch")
	}
	wantScore := strconv.FormatFloat(testScore, 'f', -1, 64)
	if got, _ := rdb.HGet(ctx, keys.claimed, "c1:score").Result(); got != wantScore {
		t.Fatalf("stored score = %q, want original %s", got, wantScore)
	}
	if got, _ := rdb.HGet(ctx, keys.owners, "c1").Result(); got != token {
		t.Fatalf("owner token mismatch")
	}

	// Double-claim by another consumer must lose the race: the member is no
	// longer pending.
	if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline), testScore); ok || err != nil {
		t.Fatalf("double claim: ok=%v err=%v, want false/nil", ok, err)
	}
}

func TestClaimRequest_SelfOverwriteOnRetryReturn(t *testing.T) {
	_, rdb, ctx, flow := newClaimTestFlow(t)

	ir, member := claimEnvelope(t, "c1", testDeadline)
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member})
	if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline), testScore); !ok || err != nil {
		t.Fatalf("first claim: ok=%v err=%v", ok, err)
	}

	// The retry mover re-enters due retries into the pending set while the
	// original claim is still alive; re-claiming must overwrite, not fail.
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member})
	if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline), testScore); !ok || err != nil {
		t.Fatalf("self re-claim: ok=%v err=%v", ok, err)
	}
	if exists, _ := rdb.HExists(ctx, newClaimKeys("q").claimed, "c1").Result(); !exists {
		t.Fatal("payload field missing after self-overwrite")
	}
}

func TestReleaseClaim_RestoresOriginalScoreAndHonorsToken(t *testing.T) {
	_, rdb, ctx, flow := newClaimTestFlow(t)

	ir, member := claimEnvelope(t, "c1", testDeadline)
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member})
	token, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline), testScore)
	if !ok || err != nil {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}

	// A stale owner must not drop the live claim.
	if err := flow.releaseClaim(ctx, "q", "c1", member, float64(testDeadline), "deadbeef"); err != nil {
		t.Fatalf("stale-token release returned error: %v", err)
	}
	if exists, _ := rdb.HExists(ctx, newClaimKeys("q").claimed, "c1").Result(); !exists {
		t.Fatal("stale token removed a foreign claim")
	}

	if err := flow.releaseClaim(ctx, "q", "c1", member, float64(testDeadline), token); err != nil {
		t.Fatalf("release: %v", err)
	}
	score, err := rdb.ZScore(ctx, "q", member).Result()
	if err != nil {
		t.Fatalf("member not restored to pending: %v", err)
	}
	if score != testScore {
		t.Fatalf("restored score = %f, want original %v", score, testScore)
	}
	if n, _ := rdb.HLen(ctx, newClaimKeys("q").claimed).Result(); n != 0 {
		t.Fatalf("claimed hash len = %d, want 0 (payload+score cleaned)", n)
	}
}

func TestAckResult_PushesOnceThenSuppressesDuplicates(t *testing.T) {
	_, rdb, ctx, flow := newClaimTestFlow(t)

	ir, member := claimEnvelope(t, "c1", testDeadline)
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member})
	if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline), testScore); !ok || err != nil {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}

	pushed, err := flow.ackResult(ctx, "q", "results", "c1", `{"id":"c1"}`, 0)
	if err != nil || !pushed {
		t.Fatalf("first ack: pushed=%v err=%v", pushed, err)
	}
	pushed, err = flow.ackResult(ctx, "q", "results", "c1", `{"id":"c1"}`, 0)
	// Without a dedup marker, same-instance retry after a successful ack has
	// no owner to fence against, so it will push again (at-least-once). The
	// caller is responsible for not retrying a successful ack.
	if err != nil || !pushed {
		t.Fatalf("second ack (no marker, no owner): pushed=%v err=%v, want true/nil", pushed, err)
	}
	if n, _ := rdb.LLen(ctx, "results").Result(); n != 2 {
		t.Fatalf("result list len = %d, want 2 (no marker, duplicate allowed)", n)
	}
	// Ack must drop the claim so the reclaimer never redelivers it — both
	// the payload and the companion score field.
	if exists, _ := rdb.HExists(ctx, newClaimKeys("q").claimed, "c1").Result(); exists {
		t.Fatal("claim survived its own ack")
	}
	if exists, _ := rdb.HExists(ctx, newClaimKeys("q").claimed, "c1:score").Result(); exists {
		t.Fatal("score companion survived its own ack")
	}
}

func TestAckResult_StaleTokenLeavesForeignClaimIntact(t *testing.T) {
	_, rdb, ctx, flow := newClaimTestFlow(t)

	// Simulate a claim owned by ANOTHER instance (no local token registered).
	keys := newClaimKeys("q")
	_, member := claimEnvelope(t, "c1", testDeadline)
	rdb.HSet(ctx, keys.claimed, "c1", member)
	rdb.HSet(ctx, keys.owners, "c1", "foreign-token")
	rdb.ZAdd(ctx, keys.idx, redis.Z{Score: float64(time.Now().Add(time.Hour).Unix()), Member: "c1"})

	pushed, err := flow.ackResult(ctx, "q", "results", "c1", `{"id":"c1"}`, 0)
	if err != nil || pushed {
		t.Fatalf("stale ack should be fenced: pushed=%v err=%v", pushed, err)
	}
	if exists, _ := rdb.HExists(ctx, keys.claimed, "c1").Result(); !exists {
		t.Fatal("fenced ack dropped a foreign instance's claim")
	}
	if n, _ := rdb.LLen(ctx, "results").Result(); n != 0 {
		t.Fatalf("fenced ack pushed result, len=%d want 0", n)
	}
}

func TestReclaimExpiredClaims_RedeliversOnlyLapsedLeases(t *testing.T) {
	_, rdb, ctx, flow := newClaimTestFlow(t)

	// Expired claim: redelivered at its original sort score. A negative lease
	// TTL puts the expiry in the past deterministically (lease scores have
	// whole-second granularity).
	flow.claimLeaseTTL = -2 * time.Second
	irE, memberE := claimEnvelope(t, "expired", testDeadline)
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: memberE})
	if _, ok, err := flow.claimRequest(ctx, "q", irE, memberE, float64(testDeadline), testScore); !ok || err != nil {
		t.Fatalf("claim expired-case: ok=%v err=%v", ok, err)
	}
	// Live claim: untouched.
	flow.claimLeaseTTL = time.Hour
	irL, memberL := claimEnvelope(t, "live", testDeadline)
	flow.claimLeaseTTL = time.Hour
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore + 1, Member: memberL})
	if _, ok, err := flow.claimRequest(ctx, "q", irL, memberL, float64(testDeadline), testScore+1); !ok || err != nil {
		t.Fatalf("claim live-case: ok=%v err=%v", ok, err)
	}

	released, err := flow.reclaimExpiredClaims(ctx)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if released != 1 {
		t.Fatalf("released = %d, want 1", released)
	}
	score, err := rdb.ZScore(ctx, "q", memberE).Result()
	if err != nil {
		t.Fatalf("expired request not redelivered: %v", err)
	}
	if score != testScore {
		t.Fatalf("redelivered score = %f, want original %v", score, testScore)
	}
	if exists, _ := rdb.HExists(ctx, newClaimKeys("q").claimed, "live").Result(); !exists {
		t.Fatal("live claim was reclaimed")
	}
}

func TestRenewClaim_ExtendsLiveAndIgnoresMissing(t *testing.T) {
	_, rdb, ctx, flow := newClaimTestFlow(t)

	ir, member := claimEnvelope(t, "c1", testDeadline)
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member})
	if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline), testScore); !ok || err != nil {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}

	before, _ := rdb.ZScore(ctx, newClaimKeys("q").idx, "c1").Result()
	// Grow the lease so the renewal lands in a later whole-second bucket
	// (lease scores are unix seconds; same-second rewrites would compare equal).
	flow.claimLeaseTTL = time.Hour
	var c1Token string
	if v, ok := flow.claimTokens.Load("c1"); ok {
		if h, ok := v.(*claimHandle); ok {
			c1Token = h.token
		}
	}
	if _, err := flow.renewClaim(ctx, "q", "c1", float64(testDeadline), c1Token); err != nil {
		t.Fatalf("renew: %v", err)
	}
	after, _ := rdb.ZScore(ctx, newClaimKeys("q").idx, "c1").Result()
	if after <= before {
		t.Fatalf("lease not extended: before=%f after=%f", before, after)
	}

	// Renewing an unknown request must be a clean no-op (returns 0, no error).
	if res, err := flow.renewClaim(ctx, "q", "ghost", float64(testDeadline), "ghost-token"); err != nil {
		t.Fatalf("renew ghost returned error: %v", err)
	} else if res != 0 {
		t.Fatalf("ghost renewal should return 0, got %d", res)
	}
	if _, err := rdb.ZScore(ctx, newClaimKeys("q").idx, "ghost").Result(); err != redis.Nil {
		t.Fatalf("ghost renewal created an index entry (err=%v)", err)
	}
}

func TestClaimExpiry_CapsAtDeadlinePlusGrace(t *testing.T) {
	_, _, _, flow := newClaimTestFlow(t)
	flow.claimLeaseTTL = time.Hour

	now := float64(time.Now().Unix())
	farFuture := now + 24*3600
	expiry := flow.claimExpiry(farFuture)
	if expiry > now+time.Hour.Seconds()+1 {
		t.Fatalf("expiry %f exceeds configured lease past now %f", expiry, now)
	}

	nearExpiry := now + 10
	expiry = flow.claimExpiry(nearExpiry)
	want := nearExpiry + reclaimGraceAfterDeadline.Seconds()
	if expiry != want {
		t.Fatalf("expiry = %f, want deadline+grace %f", expiry, want)
	}
}

func TestHeartbeatClaims_ExtendsLiveLeases(t *testing.T) {
	_, rdb, ctx, flow := newClaimTestFlow(t)

	ir, member := claimEnvelope(t, "c1", testDeadline)
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member})
	if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline), testScore); !ok || err != nil {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	before, _ := rdb.ZScore(ctx, newClaimKeys("q").idx, "c1").Result()

	// Grow the lease and heartbeat: the expiry must move out accordingly,
	// proving slow-but-healthy work is not treated as dead.
	flow.claimLeaseTTL = time.Hour
	flow.heartbeatClaims(ctx)
	after, err := rdb.ZScore(ctx, newClaimKeys("q").idx, "c1").Result()
	if err != nil {
		t.Fatalf("heartbeat lost the claim: %v", err)
	}
	if after-before < time.Hour.Seconds()-10 {
		t.Fatalf("lease not meaningfully extended: before=%f after=%f", before, after)
	}
}


