//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
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

// sortedSetCfg builds the single-queue transport config used by the loss
// tests, with optional durable-dequeue overrides.
func sortedSetCfg(addr, igwBaseURL string, leaseTTLSeconds, reclaimIntervalMs int64) redis.SortedSetConfig {
	cfg := redis.SortedSetConfig{
		URL:             "redis://" + addr,
		RetryQueueName:  "retry-sortedset",
		ResultQueueName: "result-list",
		PollIntervalMs:  50,
		BatchSize:       10,
		Queues: []redis.SortedSetQueueConfig{{
			QueueName:      "request-sortedset",
			WorkerPoolID:   "default",
			RequestPathURL: "/v1/completions",
			IGWBaseURL:     igwBaseURL,
		}},
	}
	if leaseTTLSeconds > 0 {
		cfg.ClaimLeaseTTLSeconds = leaseTTLSeconds
	}
	if reclaimIntervalMs > 0 {
		cfg.ClaimReclaimIntervalMs = reclaimIntervalMs
	}
	cfg.ApplyDefaults()
	return cfg
}

// newShutdownLossFlow builds a RedisSortedSetFlow over miniredis with a single
// queue and returns the flow plus a raw client for Redis-side accounting.
// leaseTTLSeconds/reclaimIntervalMs override the durable-dequeue tuning so
// tests can exercise lease expiry quickly (0 keeps the defaults).
func newShutdownLossFlow(t *testing.T, workers int, igwBaseURL string, leaseTTLSeconds int64, reclaimIntervalMs int64) (*redis.RedisSortedSetFlow, *goredis.Client, string) {
	t.Helper()
	s := miniredis.RunT(t)

	flow, err := redis.NewRedisSortedSetFlow(sortedSetCfg(s.Addr(), igwBaseURL, leaseTTLSeconds, reclaimIntervalMs),
		[]pipeline.WorkerPoolConfig{{ID: "default", Workers: workers}}, nil)
	require.NoError(t, err)

	rdb := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return flow, rdb, "request-sortedset"
}

// newFlowOnSameRedis builds an additional flow against an existing Redis
// (used to simulate a replacement instance taking over from a dead one).
func newFlowOnSameRedis(t *testing.T, rdb *goredis.Client, igwBaseURL string, leaseTTLSeconds, reclaimIntervalMs int64) *redis.RedisSortedSetFlow {
	t.Helper()
	flow, err := redis.NewRedisSortedSetFlow(sortedSetCfg(rdb.Options().Addr, igwBaseURL, leaseTTLSeconds, reclaimIntervalMs),
		[]pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}, nil)
	require.NoError(t, err)
	return flow
}

func enqueueShutdownLossRequests(t *testing.T, rdb *goredis.Client, queue string, ids []string) {
	t.Helper()
	ctx := context.Background()
	for _, id := range ids {
		ir := asyncapi.NewInternalRequest(
			asyncapi.InternalRouting{RequestQueueName: queue},
			&asyncapi.RequestMessage{
				ID:       id,
				Created:  time.Now().Unix(),
				Deadline: time.Now().Add(5 * time.Minute).Unix(),
				Payload:  map[string]any{"model": "test", "prompt": "hello"},
			},
		)
		member, err := ir.MarshalJSON()
		require.NoError(t, err)
		require.NoError(t, rdb.ZAdd(ctx, queue, goredis.Z{
			Score:  float64(time.Now().Add(5 * time.Minute).Unix()),
			Member: string(member),
		}).Err())
	}
}

// redisAccounting counts how many of the given ids are recoverable from Redis
// across the request queue, retry queue, and result list.
func redisAccounting(t *testing.T, rdb *goredis.Client, ids []string) (accounted int, detail string) {
	t.Helper()
	ctx := context.Background()
	inReq, err := rdb.ZRange(ctx, "request-sortedset", 0, -1).Result()
	require.NoError(t, err)
	inRetry, err := rdb.ZRange(ctx, "retry-sortedset", 0, -1).Result()
	require.NoError(t, err)
	inResult, err := rdb.LRange(ctx, "result-list", 0, -1).Result()
	require.NoError(t, err)

	var found []string
	for _, id := range ids {
		for _, m := range inReq {
			if containsID(m, id) {
				found = append(found, id+"(request-queue)")
				break
			}
		}
		for _, m := range inRetry {
			if containsID(m, id) {
				found = append(found, id+"(retry-queue)")
				break
			}
		}
		for _, m := range inResult {
			if containsID(m, id) {
				found = append(found, id+"(result-list)")
				break
			}
		}
	}
	return len(found), fmt.Sprintf("%v", found)
}

func containsID(member, id string) bool {
	return len(member) > 0 && indexOf(member, "\"id\":\""+id+"\"") >= 0
}

func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// TestGracefulShutdown_ZeroWorkers_RecoversAllPoppedRequests verifies the
// shutdown sweep: with --concurrency 0 no workers exist, so claimed requests
// strand in the merged channel and forwarder goroutine. Shutdown hands every
// unacked claim back to pending immediately — no lease wait, nothing lost.
func TestGracefulShutdown_ZeroWorkers_RecoversAllPoppedRequests(t *testing.T) {
	flow, rdb, queue := newShutdownLossFlow(t, 0, "http://localhost:30800", 0, 0)

	pools := map[string]pipeline.WorkerPoolConfig{
		"default": {ID: "default", Workers: 0},
	}
	dispatch := randomrobin.NewRandomRobinPolicy("test", randomrobin.Config{}).
		MergeRequestChannels(flow.RequestChannels(), pools)
	mergedChannel := dispatch.Channels["default"]

	ctx := context.Background()
	flow.Start(ctx)

	ids := []string{"loss-a", "loss-b", "loss-c"}
	enqueueShutdownLossRequests(t, rdb, queue, ids)

	// Wait until the first popped request reaches the merged channel buffer,
	// then give the forwarder time to pick up the next one (it will block on
	// the full merged channel, holding the request in memory only).
	waitUntil(t, 5*time.Second, func() bool { return len(mergedChannel) == 1 })
	time.Sleep(200 * time.Millisecond)

	// Graceful shutdown, exactly as runner.go does it. The sweep inside
	// Shutdown must return the stranded claims to the pending set.
	flow.StopConsuming()
	flow.Shutdown()

	accounted, detail := redisAccounting(t, rdb, ids)
	assert.Equal(t, len(ids), accounted,
		"graceful shutdown with the sweep must recover every popped request; recoverable: %s", detail)
}

// TestGracefulShutdown_WithWorkers_RecoversViaRetryQueue is the control: with
// at least one worker, drained in-memory requests flow through the retry
// channel back into the Redis retry queue during graceful shutdown.
func TestGracefulShutdown_WithWorkers_RecoversViaRetryQueue(t *testing.T) {
	serverHit := make(chan struct{}, 1)
	serverDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case serverHit <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusServiceUnavailable) // fail fast -> retry path
	}))
	defer func() {
		close(serverDone)
		server.Close()
	}()

	flow, rdb, queue := newShutdownLossFlow(t, 1, server.URL, 0, 0)

	pools := map[string]pipeline.WorkerPoolConfig{
		"default": {ID: "default", Workers: 1},
	}
	dispatch := randomrobin.NewRandomRobinPolicy("test", randomrobin.Config{}).
		MergeRequestChannels(flow.RequestChannels(), pools)
	mergedChannel := dispatch.Channels["default"]

	consumeCtx, consumeCancel := context.WithCancel(context.Background())
	requestCtx, requestCancel := context.WithCancel(context.Background())
	defer requestCancel()

	client := asyncworker.NewHTTPInferenceClient(server.Client())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		asyncworker.WorkerWithGate(consumeCtx, requestCtx, pipeline.Characteristics{},
			client, mergedChannel, flow.RetryChannel(), flow.ResultChannel(),
			5*time.Minute, nil, nil)
	}()

	ctx := context.Background()
	flow.Start(ctx)

	ids := []string{"keep-a", "keep-b", "keep-c"}
	enqueueShutdownLossRequests(t, rdb, queue, ids)

	// Deterministic progress point: the first request has been dequeued,
	// forwarded, and dispatched to the (failing) inference gateway. The other
	// two are now in-memory somewhere between Redis and the worker.
	select {
	case <-serverHit:
	case <-time.After(5 * time.Second):
		t.Fatal("first request never reached the inference gateway")
	}

	// Graceful shutdown, exactly as runner.go does it.
	flow.StopConsuming()
	consumeCancel()
	wg.Wait()
	t.Logf("retryChannel len after worker exit: %d", len(flow.RetryChannel()))
	flow.Shutdown()

	dumpQueue := func(name string) {
		members, err := rdb.ZRange(context.Background(), name, 0, -1).Result()
		if err != nil {
			t.Logf("%s: err %v", name, err)
			return
		}
		t.Logf("%s: %d members", name, len(members))
	}
	dumpQueue("retry-sortedset")
	dumpQueue("request-sortedset")

	// All three must eventually be recoverable from Redis: drained ones via
	// the retry queue, any in-transit one via the dequeue safety net.
	waitUntil(t, 5*time.Second, func() bool {
		n, _ := redisAccounting(t, rdb, ids)
		return n == len(ids)
	})
	accounted, detail := redisAccounting(t, rdb, ids)
	assert.Equal(t, len(ids), accounted, "all requests should be recoverable; found: %s", detail)
}

// TestHardKill_ReplacementFlowRedeliversClaims: a flow dies hard (no
// StopConsuming/Shutdown ever runs) while holding claims, and a replacement
// flow redelivers every claimed request so that each accepted request ends
// with exactly ONE terminal result record. The abandoned flow keeps running
// inside the test process — deliberately — and its eventual duplicate results
// must be suppressed by the terminal markers.
func TestHardKill_ReplacementFlowRedeliversClaims(t *testing.T) {
	var killHits atomic.Int64
	releaseKilled := make(chan struct{})
	killedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		killHits.Add(1)
		<-releaseKilled // block forever, like an inference that never returns
		w.WriteHeader(http.StatusOK)
	}))
	defer func() {
		close(releaseKilled)
		killedServer.Close()
	}()

	// Flow A: the doomed instance. Short lease + fast reclaim so the
	// replacement takes over quickly.
	flowA, rdbA, queue := newShutdownLossFlow(t, 1, killedServer.URL, 1, 100)

	pools := map[string]pipeline.WorkerPoolConfig{"default": {ID: "default", Workers: 1}}
	dispatchA := randomrobin.NewRandomRobinPolicy("test", randomrobin.Config{}).
		MergeRequestChannels(flowA.RequestChannels(), pools)
	mergedA := dispatchA.Channels["default"]

	clientA := asyncworker.NewHTTPInferenceClient(killedServer.Client())
	go asyncworker.WorkerWithGate(context.Background(), context.Background(),
		pipeline.Characteristics{}, clientA, mergedA, flowA.RetryChannel(), flowA.ResultChannel(),
		time.Minute, nil, nil)

	flowA.Start(context.Background())

	ids := []string{"kill-a", "kill-b", "kill-c"}
	enqueueShutdownLossRequests(t, rdbA, queue, ids)

	// Deterministic progress point: A has claimed every request and its
	// worker is blocked inside the never-returning inference call.
	waitUntil(t, 5*time.Second, func() bool { return killHits.Load() >= 1 })
	claimedKey := queue + ":claimed"
	for _, id := range ids {
		exists, err := rdbA.HExists(context.Background(), claimedKey, id).Result()
		require.NoError(t, err)
		require.True(t, exists, "flow A should hold claim for %s", id)
	}

	// ☠ HARD KILL: no StopConsuming, no Shutdown. The claims simply lapse.

	// Replacement flow C: healthy IGW returning success immediately.
	successServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"success"}`))
	}))
	defer successServer.Close()

	flowC := newFlowOnSameRedis(t, rdbA, successServer.URL, 1, 100)
	dispatchC := randomrobin.NewRandomRobinPolicy("test", randomrobin.Config{}).
		MergeRequestChannels(flowC.RequestChannels(), pools)
	mergedC := dispatchC.Channels["default"]

	clientC := asyncworker.NewHTTPInferenceClient(successServer.Client())
	var wgC sync.WaitGroup
	wgC.Add(1)
	go func() {
		defer wgC.Done()
		asyncworker.WorkerWithGate(context.Background(), context.Background(),
			pipeline.Characteristics{}, clientC, mergedC, flowC.RetryChannel(), flowC.ResultChannel(),
			time.Minute, nil, nil)
	}()
	flowC.Start(context.Background())

	// Every accepted request must produce exactly one terminal record.
	waitUntil(t, 15*time.Second, func() bool {
		n, err := rdbA.LLen(context.Background(), "result-list").Result()
		return err == nil && n == int64(len(ids))
	})
	time.Sleep(300 * time.Millisecond) // let any late duplicates attempt to land
	n, _ := rdbA.LLen(context.Background(), "result-list").Result()
	assert.Equal(t, int64(len(ids)), n, "exactly one terminal record per accepted request")

	raw, _ := rdbA.LRange(context.Background(), "result-list", 0, -1).Result()
	got := map[string]int{}
	for _, m := range raw {
		for _, id := range ids {
			if containsID(m, id) {
				got[id]++
			}
		}
	}
	for _, id := range ids {
		assert.Equal(t, 1, got[id], "request %s must have exactly one result", id)
	}
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
