//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	randomrobin "github.com/llm-d/llm-d-async/pkg/async/mergepolicy/randomrobin"

	"github.com/alicebob/miniredis/v2"
	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/asyncworker"
	"github.com/llm-d/llm-d-async/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	durabilityQueue     = "chaos-request-sortedset"
	durabilityResults   = "chaos-result-list"
	durabilityClaimed   = durabilityQueue + ":claimed"
	durabilityInstances = 3
)

// durabilityCfg isolates the chaos fixtures on their own queue/retry/result
// namespace with a deliberately tiny claim lease.
func durabilityCfg(addr string) redis.SortedSetConfig {
	cfg := redis.SortedSetConfig{
		URL:                    "redis://" + addr,
		RetryQueueName:         "chaos-retry-sortedset",
		ResultQueueName:        durabilityResults,
		PollIntervalMs:         50,
		BatchSize:              10,
		ClaimLeaseTTLSeconds:   1,
		ClaimReclaimIntervalMs: 100,
		Queues: []redis.SortedSetQueueConfig{{
			QueueName:      durabilityQueue,
			WorkerPoolID:   "default",
			RequestPathURL: "/v1/completions",
			IGWBaseURL:     "http://localhost:30800", // never reached: workers use their own client
		}},
	}
	cfg.ApplyDefaults()
	return cfg
}

func durabilityNewFlow(t *testing.T, addr string, workers int) *redis.RedisSortedSetFlow {
	t.Helper()
	flow, err := redis.NewRedisSortedSetFlow(durabilityCfg(addr),
		[]pipeline.WorkerPoolConfig{{ID: "default", Workers: workers}}, nil)
	require.NoError(t, err)
	return flow
}

// durabilityStartWorker attaches the merge policy and one worker consuming the
// flow's merged channel through the given inference server. The returned
// cancel stops the worker.
func durabilityStartWorker(flow *redis.RedisSortedSetFlow, server *httptest.Server) context.CancelFunc {
	pools := map[string]pipeline.WorkerPoolConfig{"default": {ID: "default", Workers: 1}}
	dispatch := randomrobin.NewRandomRobinPolicy("test", randomrobin.Config{}).
		MergeRequestChannels(flow.RequestChannels(), pools)
	ctx, cancel := context.WithCancel(context.Background())
	client := asyncworker.NewHTTPInferenceClient(server.Client())
	go func() {
		asyncworker.WorkerWithGate(ctx, ctx, pipeline.Characteristics{}, client, dispatch.Channels["default"],
			flow.RetryChannel(), flow.ResultChannel(), time.Minute, nil, nil)
		cancel()
	}()
	return cancel
}

// chaosTeardown stops a flow and its worker deterministically so no goroutine
// outlives the test against a closed Redis server.
func chaosTeardown(t *testing.T, flow *redis.RedisSortedSetFlow, stopWorker context.CancelFunc) {
	t.Helper()
	stopWorker()
	flow.StopConsuming()
	flow.Shutdown()
}

func durabilityEnqueue(t *testing.T, rdb *goredis.Client, ids []string) {
	t.Helper()
	ctx := context.Background()
	pipe := rdb.Pipeline()
	for _, id := range ids {
		m := asyncapi.RequestMessage{
			ID:       id,
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(10 * time.Minute).Unix(),
			Payload:  map[string]any{"model": id, "prompt": "p"},
		}
		ir := asyncapi.NewInternalRequest(asyncapi.InternalRouting{RequestQueueName: durabilityQueue}, &m)
		b, err := ir.MarshalJSON()
		require.NoError(t, err)
		pipe.ZAdd(ctx, durabilityQueue, goredis.Z{Score: float64(m.Deadline), Member: string(b)})
	}
	_, err := pipe.Exec(ctx)
	require.NoError(t, err)
}

func durabilityCounts(t *testing.T, rdb *goredis.Client) map[string]int {
	t.Helper()
	raw, err := rdb.LRange(context.Background(), durabilityResults, 0, -1).Result()
	require.NoError(t, err)
	counts := map[string]int{}
	for _, e := range raw {
		var res asyncapi.ResultMessage
		if json.Unmarshal([]byte(e), &res) == nil && res.ID != "" {
			counts[res.ID]++
		}
	}
	return counts
}

func chaosClient(t *testing.T, addr string) *goredis.Client {
	t.Helper()
	c := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// awaitExactlyOneRecordEach blocks until every id has exactly one terminal record.
func awaitExactlyOneRecordEach(t *testing.T, rdb *goredis.Client, ids []string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		counts := durabilityCounts(t, rdb)
		done := true
		for _, id := range ids {
			if counts[id] != 1 {
				done = false
				break
			}
		}
		if done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("requests did not settle to exactly-one within %v; sample=%v", timeout, counts)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func waitDurabilityCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("chaos condition not met in time")
}

// TestMultiInstance_RacingClaims_ProduceExactlyOneRecordEach hammers one queue with three
// simultaneous instances racing the atomic claim script: every request must
// complete exactly once with no duplicate terminal records.
func TestMultiInstance_RacingClaims_ProduceExactlyOneRecordEach(t *testing.T) {
	s := miniredis.RunT(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	}))
	defer server.Close()

	for i := 0; i < durabilityInstances; i++ {
		flow := durabilityNewFlow(t, s.Addr(), 2)
		flow.Start(context.Background())
		stop := durabilityStartWorker(flow, server)
		t.Cleanup(func() { chaosTeardown(t, flow, stop) })
	}

	const total = 60
	ids := make([]string, total)
	for i := range ids {
		ids[i] = fmt.Sprintf("chaos-c-%d", i)
	}
	durabilityEnqueue(t, chaosClient(t, s.Addr()), ids)
	awaitExactlyOneRecordEach(t, chaosClient(t, s.Addr()), ids, 45*time.Second)

	counts := durabilityCounts(t, chaosClient(t, s.Addr()))
	assert.Len(t, counts, total, "every request must produce a record")
}

// TestRepeatedTakeoverCycles_UnderSustainedLoad runs repeated
// claim-strand-sweep-survive cycles against one persistent survivor: every
// request stranded by a dying generation must resurface (via sweep or lease
// expiry) and complete exactly once.
func TestRepeatedTakeoverCycles_UnderSustainedLoad(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := chaosClient(t, s.Addr())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	}))
	defer server.Close()

	survivor := durabilityNewFlow(t, s.Addr(), 3)
	survivor.Start(context.Background())
	stopSurvivor := durabilityStartWorker(survivor, server)
	t.Cleanup(func() { chaosTeardown(t, survivor, stopSurvivor) })

	ctx := context.Background()
	const rounds = 4
	const perRound = 5
	allIDs := make([]string, 0, rounds*perRound)

	for round := 0; round < rounds; round++ {
		// A doomed generation claims requests and strands them in memory.
		victim := durabilityNewFlow(t, s.Addr(), 0)
		victim.Start(ctx)

		roundIDs := make([]string, perRound)
		for i := range roundIDs {
			roundIDs[i] = fmt.Sprintf("chaos-k-%d-%d", round, i)
		}
		allIDs = append(allIDs, roundIDs...)
		durabilityEnqueue(t, rdb, roundIDs)

		// Wait until the victim holds every request as a claim.
		waitDurabilityCondition(t, 5*time.Second, func() bool {
			n, err := rdb.ZCard(ctx, durabilityQueue).Result()
			return err == nil && n == 0
		})

		// Graceful teardown with the full sweep: stranded claims return to
		// pending immediately for the survivor to pick up.
		victim.StopConsuming()
		victim.Shutdown()

		awaitExactlyOneRecordEach(t, rdb, roundIDs, 20*time.Second)
	}

	awaitExactlyOneRecordEach(t, rdb, allIDs, 10*time.Second)
	claimed, err := rdb.HLen(ctx, durabilityClaimed).Result()
	require.NoError(t, err)
	assert.Zero(t, claimed, "no claim may outlive its terminal record")
	pending, err := rdb.ZCard(ctx, durabilityQueue).Result()
	require.NoError(t, err)
	assert.Zero(t, pending, "queue must be fully drained")
	retried, err := rdb.ZCard(ctx, "chaos-retry-sortedset").Result()
	require.NoError(t, err)
	assert.Zero(t, retried, "retry queue must be empty after settle")
}

// TestDurabilityBackgroundLoops_SurviveRedisDown mirrors asynq's
// TestHeartbeaterWithRedisDown: the heartbeater and reclaimer must log their
// errors and keep looping — never panic or wedge shutdown — while Redis is
// unreachable.
func TestDurabilityBackgroundLoops_SurviveRedisDown(t *testing.T) {
	s := miniredis.RunT(t)
	flow := durabilityNewFlow(t, s.Addr(), 0)
	flow.Start(context.Background())

	// Simulate total Redis loss mid-flight.
	s.Close()

	// Give both background loops several ticks against the dead server.
	time.Sleep(400 * time.Millisecond)

	// Shutdown must still terminate even though every Redis call fails.
	done := make(chan struct{})
	go func() {
		flow.StopConsuming()
		flow.Shutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("shutdown wedged while Redis was down")
	}
}

// TestReclaimExpiredClaims_DrainLargeBacklog pins the pagination behavior
// peers solve with XAUTOCLAIM cursors: more expired claims than one pass's
// batch size must still drain completely across successive passes, each
// request restored exactly once.
func TestReclaimExpiredClaims_DrainLargeBacklog(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	flow := durabilityNewFlow(t, s.Addr(), 0)

	const total = 150 // > reclaimBatchSize (100)
	ctx := context.Background()
	deadline := float64(time.Now().Add(time.Hour).Unix())
	expectedScores := map[string]float64{}
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("backlog-%d", i)
		m := asyncapi.RequestMessage{
			ID:       id,
			Created:  time.Now().Unix(),
			Deadline: int64(deadline),
			Payload:  map[string]any{"model": id, "prompt": "p"},
		}
		ir := asyncapi.NewInternalRequest(asyncapi.InternalRouting{RequestQueueName: durabilityQueue}, &m)
		b, err := ir.MarshalJSON()
		require.NoError(t, err)
		// Seed claims directly in an already-expired state (score < now),
		// bypassing claimRequest so the whole backlog ages simultaneously.
		pipe := rdb.Pipeline()
		pipe.HSet(ctx, durabilityQueue+":claimed", id, string(b))
		pipe.HSet(ctx, durabilityQueue+":claimed", id+":score", fmt.Sprintf("%d", i))
		expectedScores[id] = float64(i)
		pipe.HSet(ctx, durabilityQueue+":claim-owners", id, "seed-token")
		pipe.ZAdd(ctx, durabilityQueue+":claims-idx", goredis.Z{
			Score:  float64(time.Now().Add(-time.Hour).Unix()),
			Member: id,
		})
		_, err = pipe.Exec(ctx)
		require.NoError(t, err)
	}

	flow.Start(context.Background())
	waitDurabilityCondition(t, 15*time.Second, func() bool {
		hLen, hErr := rdb.HLen(ctx, durabilityQueue+":claimed").Result()
		pLen, pErr := rdb.ZCard(ctx, durabilityQueue).Result()
		return hErr == nil && pErr == nil && hLen == 0 && pLen == total
	})

	// Every restored member must sit in pending at its seeded score, once.
	restored := map[string][]float64{}
	for _, z := range rdb.ZRangeWithScores(ctx, durabilityQueue, 0, -1).Val() {
		var ir asyncapi.InternalRequest
		if json.Unmarshal([]byte(z.Member.(string)), &ir) != nil {
			continue
		}
		id := ir.PublicRequest.ReqID()
		restored[id] = append(restored[id], z.Score)
	}
	for id, want := range expectedScores {
		require.Len(t, restored[id], 1, "request %s must be restored exactly once", id)
		assert.Equal(t, want, restored[id][0], "request %s must restore at its seeded score", id)
	}
}
