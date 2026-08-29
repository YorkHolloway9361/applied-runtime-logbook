# How to Ship Large ZIP Exports with Go Workers, Multipart Upload, and Signed Links

Short answer: stream the archive from a worker into a multipart object upload, publish a manifest only after completion, and issue a short-lived signed download link from that immutable object. Alert on export age and failed-part rate, not on a vague “queue is busy” gauge.

The page usually arrives late. A media editor clicks “export,” the job enters a queue, and twenty minutes later the customer reports that the download link is missing. The on-call view shows workers healthy, CPU low, and a growing set of objects named `tmp/export-*.zip`. That is a storage handoff failure hiding behind a healthy queue.

Measure the handoff.

I treat this as an alert-to-action trace. The useful signal is the age of the oldest export that has not reached a terminal state. Once that crosses the product deadline, the page should include the job ID, upload ID, completed part count, and last object-store response class. Those fields turn a midnight investigation into a bounded runbook.

## Decide the export deadline before the first byte

At minute zero, the API records an export ID and returns a job status URL. The worker claims that ID, starts a multipart upload, and emits progress after each part. Near the deadline, the useful question is not “is the worker alive?” but “which state transition stopped?” If bytes are increasing while the state remains `writing`, the archive producer is the suspect. If bytes stop at an exact part boundary, inspect the upload call and its retry budget. If all parts exist but the state is still `uploading`, inspect completion and the conditional commit. This timeline makes the alert actionable because every observation maps to one owner and one next command; it also prevents a well-meaning responder from issuing a second export that doubles CPU and storage load.

The clock starts at commit.

## The commit record is the contract

ZIP creation and object upload have different failure domains. Compression can keep making progress while a part upload is retried forever, or a worker can die after the final part but before the multipart completion call. A queue metric sees neither distinction.

The first design rule is to make state explicit: `queued`, `writing`, `uploading`, `committed`, `expired`, or `failed`. Store the upload ID and part numbers in durable job state. A retry must resume from that state or abandon the upload deliberately; it must not create a second public object with the same logical export ID.

The second rule is idempotency. Give each export a stable object key such as `exports/{tenant}/{exportID}.zip`, and keep the completion record separate from the object bytes. Readers only receive a link after the record says `committed`. An incomplete upload is not a downloadable artifact.

Object storage multipart APIs generally require a minimum part size except for the final part, plus an ordered list of part numbers and checksums at completion. The exact limit differs by service, so the worker should read it from configuration and test it against the selected backend. Do not infer a limit from a local filesystem test.

That record is the product contract. A link endpoint should refuse to sign an object unless the conditional commit has succeeded, even if the bytes happen to exist.

## What should a Go worker record before it writes a large ZIP to object storage?

The worker below uses a small interface so the policy is testable without a cloud SDK. The production adapter maps these methods to the chosen object-storage API. The archive bytes never pass through the application HTTP server.

```go
package export

import (
	"context"
	"fmt"
	"io"
	"time"
)

type Store interface {
	Begin(ctx context.Context, key string) (uploadID string, err error)
	PutPart(ctx context.Context, key, uploadID string, number int, body io.Reader) (etag string, err error)
	Complete(ctx context.Context, key, uploadID string, parts []string) error
	Abort(ctx context.Context, key, uploadID string) error
	SignedGet(ctx context.Context, key string, ttl time.Duration) (string, error)
}

func writeExport(ctx context.Context, store Store, key string, zip io.Reader, partSize int64) (string, error) {
	uploadID, err := store.Begin(ctx, key)
	if err != nil {
		return "", err
	}
	completed := make([]string, 0, 16)
	part := 1
	buf := make([]byte, partSize)

	for {
		n, readErr := io.ReadFull(zip, buf)
		if readErr != nil && readErr != io.ErrUnexpectedEOF && readErr != io.EOF {
			_ = store.Abort(ctx, key, uploadID)
			return "", readErr
		}
		if n == 0 {
			break
		}

		var putErr error
		for attempt := 1; attempt <= 3; attempt++ {
			_, putErr = store.PutPart(ctx, key, uploadID, part, io.NewSectionReader(readerAt(buf[:n]), 0, int64(n)))
			if putErr == nil {
				break
			}
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
		}
		if putErr != nil {
			_ = store.Abort(ctx, key, uploadID)
			return "", fmt.Errorf("part %d: %w", part, putErr)
		}
		completed = append(completed, fmt.Sprintf("%d", part))
		part++
		if readErr == io.ErrUnexpectedEOF || readErr == io.EOF {
			break
		}
	}

	if err := store.Complete(ctx, key, uploadID, completed); err != nil {
		_ = store.Abort(ctx, key, uploadID)
		return "", err
	}
	return store.SignedGet(ctx, key, 30*time.Minute)
}

// readerAt is a minimal adapter used to keep the example SDK-neutral.
type byteReaderAt []byte
func (b byteReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(b)) { return 0, io.EOF }
	n := copy(p, b[off:])
	if n < len(p) { return n, io.EOF }
	return n, nil
}
func readerAt(b []byte) io.ReaderAt { return byteReaderAt(b) }
```

The retry loop is intentionally boring. In a real worker, replace the sleep with bounded backoff and context cancellation, and record the attempt count. A failed completion is different from a failed part: the job can safely retry completion with the same ordered list, while a part failure may require re-reading the ZIP stream or restarting the upload.

Do not hide that distinction in a generic `retry_count`. Keep `part_attempts`, `completion_attempts`, and `last_progress_at` separate; those fields tell an on-call whether to wait, resume, or abort.

The signed URL is a capability, not an authorization database. Bind it to the committed key, choose a short expiry, and log issuance without logging the full query string. If the export is sensitive, put tenant and export identifiers in the authorization check before signing, then let the object store serve the bytes directly.

## When does a media export become page-worthy?

Page on a customer-visible deadline: `now - created_at > export_sla` for a non-terminal job. Add a second condition for a high failed-part ratio over five minutes. The first catches silent stalls; the second catches a backend or network degradation before the SLA is missed.

The alert payload should link to one job record and expose four counters: bytes read, bytes uploaded, parts completed, and seconds since the last progress event. A worker heartbeat alone is weak evidence. A process can heartbeat while repeatedly re-encoding the same first gigabyte.

I'm not sure a single global threshold can work for every media tier. A ten-minute SD clip and a multi-hour 4K package have different normal curves, so the alert needs the export's expected size and deadline beside the observed age.

When paged, I walk the trace in this order:

1. Confirm the job state transition and the deadline timestamp.
2. Check whether the upload ID has a recent part and whether part numbers are contiguous.
3. Compare uploaded bytes with the ZIP writer's read offset.
4. If completion succeeded, verify the object metadata and manifest before regenerating a link.
5. Abort abandoned uploads and mark the job for a controlled retry, preserving the original export ID.

This sequence also catches duplicate delivery. A queue redelivery is harmless when the worker claims the job with a lease and the commit record uses a conditional write. Without that guard, two workers can complete different uploads and the last writer wins while the customer downloads an unpredictable archive.

## Reliability depends on retention cleanup

False positives have a cost. A fixed ten-minute threshold pages on every unusually large 4K package; a threshold based only on average throughput misses a wedged part upload. Use a size-aware deadline: an expected compression/upload duration plus a fixed margin, capped by the customer promise. Record the estimate used for each job so an incident review can tell whether the threshold itself was wrong.

The catch is operational complexity. If your team cannot retain per-part state or run lifecycle cleanup, a direct single-stream upload may be safer for smaller artifacts, and a managed batch export service may suit a high-volume catalog. Stick with the simpler path when exports fit comfortably inside one request's timeout and resumability is not a requirement. Multipart workers are a poor fit when you cannot guarantee a durable job store, because recovery becomes guesswork.

Object lifecycle rules should delete abandoned multipart uploads and expired export objects. Keep the retention window aligned with the longest signed-link lifetime, plus a review buffer. Test deletion in a staging bucket; an overly broad rule can erase a still-valid customer download.

## References

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
- https://cloud.google.com/storage/docs
- https://datatracker.ietf.org/doc/html/rfc7231
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/HEAD
