package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

const (
	hardKillRequestQueue = "hardkill-request-sortedset"
	hardKillResultQueue  = "hardkill-result-list"
	hardKillClaimedKey   = hardKillRequestQueue + ":claimed"
	hardKillRelease      = "hard-kill"
)

// forceDeleteProcessorPods deletes the processor pods of a helm release with
// zero grace period, simulating SIGKILL/OOM/node loss: no shutdown handler,
// no drain — claims simply lapse.
func forceDeleteProcessorPods(release string) {
	cmd := exec.Command("kubectl", "--kubeconfig", kindKubeconfig, "-n", nsName,
		"delete", "pods",
		"-l", "app.kubernetes.io/instance="+release+",app.kubernetes.io/name=llm-d-async",
		"--force", "--grace-period=0", "--wait=false")
	sess, err := gexec.Start(cmd, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(sess).WithTimeout(30 * time.Second).Should(gexec.Exit(0))
}

// waitProcessorRollout blocks until the release's replacement pod is ready.
func waitProcessorRollout(release string) {
	cmd := exec.Command("kubectl", "--kubeconfig", kindKubeconfig, "-n", nsName,
		"rollout", "status", "deployment/"+release+"-llm-d-async", "--timeout=120s")
	sess, err := gexec.Start(cmd, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(sess).WithTimeout(130 * time.Second).Should(gexec.Exit(0))
}

// countResultsByID tallies non-destructively how many terminal records each
// request id has in the result list.
func countResultsByID(ctx context.Context, queue string) map[string]int {
	raw, err := rdb.LRange(ctx, queue, 0, -1).Result()
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	counts := map[string]int{}
	for _, entry := range raw {
		var res api.ResultMessage
		if json.Unmarshal([]byte(entry), &res) != nil {
			continue
		}
		counts[res.ID]++
	}
	return counts
}

var _ = ginkgo.Describe("Hard kill durability", func() {
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		rdb.Del(ctx, hardKillRequestQueue)
		rdb.Del(ctx, hardKillResultQueue)
		rdb.Del(ctx, hardKillClaimedKey)
		rdb.Del(ctx, hardKillRequestQueue+":claims-idx")
		rdb.Del(ctx, hardKillRequestQueue+":claim-owners")
	})

	ginkgo.AfterEach(func() {
		setEnvoyFaultDelay(envoyAdminURL, 0)
	})

	ginkgo.It("redelivers claimed requests after a force pod deletion with exactly one result each", func() {
		// Unique ids per run: terminal dedup markers live up to
		// result_dedup_ttl on shared Redis, so a repeated id would be
		// correctly suppressed as a duplicate by a later run.
		run := time.Now().UnixNano()
		ids := []string{
			fmt.Sprintf("hardkill-%d-1", run),
			fmt.Sprintf("hardkill-%d-2", run),
			fmt.Sprintf("hardkill-%d-3", run),
			fmt.Sprintf("hardkill-%d-4", run),
		}
		msgs := make([]api.RequestMessage, 0, len(ids))
		for _, id := range ids {
			msgs = append(msgs, makeRequestMessage(id, 10*time.Minute))
		}
		enqueueMessages(ctx, rdb, hardKillRequestQueue, msgs...)

		// Hold every request in-flight at the envoy fault filter so the
		// processor claims them all while none can complete.
		setEnvoyFaultDelay(envoyAdminURL, 100)

		// HLen counts both fields per request (payload + ":score"), so
		// assert claimed-ness per id instead.
		gomega.Eventually(func() int64 {
			claimed := int64(0)
			for _, id := range ids {
				if rdb.HExists(ctx, hardKillClaimedKey, id).Val() {
					claimed++
				}
			}
			return claimed
		}, 60*time.Second, 500*time.Millisecond).Should(gomega.Equal(int64(len(ids))),
			"all requests should be claimed (dequeued under lease)")
		gomega.Expect(rdb.ZCard(ctx, hardKillRequestQueue).Val()).To(gomega.Equal(int64(0)),
			"claimed requests must be out of the pending set")
		gomega.Consistently(func() int64 {
			return rdb.LLen(ctx, hardKillResultQueue).Val()
		}, 3*time.Second, 500*time.Millisecond).Should(gomega.Equal(int64(0)),
			"in-flight requests must not produce results yet")

		// Kill the processor with zero grace period: no StopConsuming, no
		// Shutdown, no sweep. Claims lapse and the replacement's reclaimer
		// must redeliver them.
		forceDeleteProcessorPods(hardKillRelease)
		setEnvoyFaultDelay(envoyAdminURL, 0)
		waitProcessorRollout(hardKillRelease)

		// Every accepted request ends with exactly one terminal record.
		gomega.Eventually(func() int64 {
			return rdb.LLen(ctx, hardKillResultQueue).Val()
		}, 120*time.Second, time.Second).Should(gomega.Equal(int64(len(ids))),
			"one terminal record per accepted request")

		counts := countResultsByID(ctx, hardKillResultQueue)
		for _, id := range ids {
			gomega.Expect(counts[id]).To(gomega.Equal(1),
				"request %s must have exactly one terminal record, got %v", id, counts)
		}
		gomega.Expect(rdb.HLen(ctx, hardKillClaimedKey).Val()).To(gomega.Equal(int64(0)),
			"no claim may outlive its terminal record")
		gomega.Expect(rdb.ZCard(ctx, hardKillRequestQueue).Val()).To(gomega.Equal(int64(0)),
			"redelivered requests must be consumed, not left pending")

		// The settled batch stays stable: no late duplicates from laggards.
		gomega.Consistently(func() map[string]int {
			return countResultsByID(ctx, hardKillResultQueue)
		}, 5*time.Second, 500*time.Millisecond).Should(gomega.HaveLen(len(ids)))
	})
})
