# Nightly Payment Reconciliation: Node.js Failed Webhooks, Backoff, and DLQ Redrive

Short answer: for a nightly payment reconciliation, use an at-least-once retry queue, exponential backoff with jitter, and a dead-letter queue (DLQ), while making the payment write idempotent by event ID. The queue controls delivery pressure; it cannot make a duplicate delivery safe.

That distinction is the first line in my runbook. A payment provider can accept a request while the worker loses its response. A worker can then retry a request that already changed the ledger. “Delivered once” is not a useful promise at this boundary. The useful promise is that an event may be delivered more than once, but the business effect is applied once.

Don't guess.

This matters even if the worker is written in Node.js. The language changes the client library and process model. It does not change the failure modes: an acknowledgement can be lost, a receiver can be slow, and a poison payload can consume every retry slot.

## What should a webhook retry queue do for failed webhooks, exponential backoff, and DLQ redrive?

Put a stable event ID, destination, attempt number, and payload reference in every job. The worker should classify the outcome before it chooses a state transition. A temporary receiver rejection or a network timeout is retryable. A bad signature, invalid payload mapping, or a permanent authorization decision needs quarantine and human review. Do not turn every error into a retry.

The safe ordering is deliberately conservative:

1. Read one job with a visibility timeout.
2. Attempt delivery with the event ID and a signed body.
3. On success, or on a durable duplicate classification, acknowledge the job.
4. On a retryable result, publish a successor with an incremented attempt and a future ready time, then acknowledge the current job.
5. At the attempt ceiling, publish the job to the DLQ before acknowledging the current job.

The publish-before-ack sequence can briefly leave two copies if the worker is interrupted after publishing. That is an acceptable overlap. Losing the only copy is worse, and the idempotency record at the payment write boundary is what makes the overlap harmless. Keep that record durable and unique on the event ID; a retry counter is operational metadata, not a deduplication system.

The failure I design around is boring and expensive: the worker sends the reconciliation request, the provider commits the payment update, and the connection closes before the worker receives the response. The worker has no honest way to distinguish that from a request that never arrived. It should therefore retry with the same event ID, let the receiving write boundary decide whether the ID was already applied, and record the outcome as a duplicate when it was. A queue setting cannot resolve that ambiguity. The event ID, the provider's idempotency contract if one exists, and the local uniqueness record have to agree on what counts as the same operation. If those three identifiers drift, the dashboard may show healthy delivery while the ledger contains a duplicate adjustment.

Use exponential delay with a cap and random jitter. If every failed webhook becomes ready at exactly the same interval, a recovering payment provider receives a synchronized wave. Your mileage may vary on the cap: it should follow the provider’s documented recovery behavior and rate limits, then be checked against the reconciliation deadline.

## A small worker state machine for delayed payment delivery

The example below is a transport-neutral model of the worker. The production implementation may use a Node.js queue client, but the state transitions are the part worth testing. The `applied` map stands for a durable uniqueness constraint; an in-memory map is only suitable for this small exercise.

```go
package main

import (
	"fmt"
	"math/rand"
	"time"
)

type Job struct {
	EventID string
	Attempt int
	ReadyAt time.Time
}

type Worker struct {
	queue   []Job
	dlq     []Job
	applied map[string]bool
	healthy bool
}

func backoff(attempt int, random *rand.Rand) time.Duration {
	base := time.Second << min(attempt, 5)
	if base > 32*time.Second {
		base = 32 * time.Second
	}
	jitter := time.Duration(random.Int63n(int64(base / 4)))
	return base + jitter
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (w *Worker) deliver(job Job) error {
	if w.applied[job.EventID] {
		return nil
	}
	if !w.healthy {
		return fmt.Errorf("receiver did not accept %s", job.EventID)
	}
	w.applied[job.EventID] = true
	return nil
}

func (w *Worker) run(maxAttempts int, random *rand.Rand) {
	for len(w.queue) > 0 {
		job := w.queue[0]
		w.queue = w.queue[1:]
		if wait := time.Until(job.ReadyAt); wait > 0 {
			time.Sleep(wait)
		}
		if w.deliver(job) == nil {
			continue
		}
		job.Attempt++
		if job.Attempt >= maxAttempts {
			w.dlq = append(w.dlq, job)
			continue
		}
		job.ReadyAt = time.Now().Add(backoff(job.Attempt, random))
		w.queue = append(w.queue, job)
	}
}

func (w *Worker) redrive() {
	for _, job := range w.dlq {
		job.Attempt = 0
		job.ReadyAt = time.Now()
		w.queue = append(w.queue, job)
	}
	w.dlq = nil
}

func main() {
	random := rand.New(rand.NewSource(7))
	w := Worker{applied: map[string]bool{}}
	w.queue = append(w.queue, Job{EventID: "payment-1042", ReadyAt: time.Now()})
	w.run(3, random)
	fmt.Printf("quarantined: %d\n", len(w.dlq))
	w.healthy = true
	w.redrive()
	w.run(3, random)
	w.queue = append(w.queue, Job{EventID: "payment-1042", ReadyAt: time.Now()})
	w.run(3, random)
	fmt.Printf("applied events: %d\n", len(w.applied))
}
```

The `deliver` method returns an error only to make the failed-receiver path visible. In a real worker, keep outcome classification separate from transport details. Record the event ID, attempt, destination, result class, and next ready time. Do not put secrets in the job body. HMAC provides a keyed message-authentication construction; RFC 2104 is the relevant standard reference for the signing primitive.

One subtlety deserves emphasis: redrive is a state transition, not a button that means “try everything again.” Resetting attempts can be reasonable after a confirmed receiver repair, but only for a selected cohort. Preserve the original event ID and failure reason so an operator can explain why a payment was replayed.

## Which delivery guarantee fits a nightly reconciliation?

The answer depends on what the reconciliation is allowed to do after a timeout. For payment updates, choose at-least-once delivery plus an idempotent consumer. Exactly-once processing across a worker, a network, and an external provider is not something a queue acknowledgement can establish.

| Requirement | Queue design | Operational consequence |
|---|---|---|
| A missed payment update must be recoverable | Main queue plus DLQ | Retain the original event and inspect DLQ age, not only depth |
| A repeated request must not double-apply | Durable uniqueness on event ID | Test the timeout-after-commit case explicitly |
| A provider needs recovery time | Capped exponential backoff with jitter | Measure oldest ready job against the reconciliation deadline |
| A bad payload must stop cycling | Permanent-failure classification | Redrive only after the payload or mapping is corrected |
| A nightly batch must finish predictably | Per-destination limits and a deadline alert | Isolate a slow provider from unrelated deliveries |

The queue is a poor fit for a multi-step workflow with dependencies, joins, or a requirement to resume a long business process from named checkpoints. Use a workflow-oriented design for that shape of work. A queue is a good fit when each webhook delivery is an independent unit and the receiver’s idempotency boundary is clear.

Keep the boundary explicit.

The catch is operational ownership. A queue adds worker deployment, visibility-timeout tuning, metrics, and replay controls. It is not suitable when the team cannot preserve a durable event record or cannot investigate a DLQ before retention expires. In that case, a simpler scheduled reconciliation that reads the provider’s source of truth may be safer, even if it is less immediate.

## Verify, alert, and roll back without losing evidence

Before release, exercise four paths: accepted delivery, duplicate delivery, retryable failure, and permanent failure. The duplicate test should simulate a receiver commit followed by a lost response. The expected result is one ledger write for one event ID, with the later attempt classified as already applied.

I watch two clocks: the age of the oldest ready job and the time remaining before the nightly reconciliation deadline. I also split DLQ depth by destination and failure class. A rising main queue says delivery capacity or receiver availability is constrained. A rising DLQ says automatic retry has stopped being a useful decision. Those signals need different responses.

During recovery, stop automatic redrive, fix the receiver or classifier, and replay a small cohort. Check acceptance, ledger uniqueness, queue age, retry counts, and DLQ depth before increasing the batch. Keep the cohort intact while investigating. A quiet dashboard is not proof of recovery.

Rollback means stop new publishing from the changed worker, return to the prior worker version, and preserve queued jobs, event IDs, and the idempotency boundary. Do not purge evidence during diagnosis. After the correction is verified, redrive in bounded batches and record the operator decision. Slow is fine here. Irreversible is not.

## References

- https://www.rfc-editor.org/rfc/rfc2104
- https://github.com/sidekiq/sidekiq/wiki
