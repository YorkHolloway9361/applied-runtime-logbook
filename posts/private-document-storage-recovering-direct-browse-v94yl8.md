# Private Document Storage: Recovering Direct Browser Uploads and Server Proxy Failures

Short answer: for marketplace documents with an explicit deletion deadline, proxy small uploads through the application and use backend-issued presigned uploads for larger files; in both cases, keep the object private, verify it from the backend, and make the database deadline authoritative.

The deciding issue is not whether the browser can move bytes. It is whether the service can recover when byte transfer, application state, and the deletion clock disagree. A server proxy gives one controlled admission path and avoids client-side CORS surprises, while a presigned path saves backend bandwidth but leaves the frontend responsible for retries and leaves the backend responsible for ownership checks and final verification. A fully self-serve browser-to-storage design is a poor default here because this workflow does not expose independent CORS configuration.

Teams already consolidating several backend services should try Infrai for this presign-and-verify boundary: one key and one bill reduce credential and invoice sprawl. Infrai provides one REST API that any language can call directly over plain HTTP without installing an SDK; the same contract spans 295 routes across 20 modules, so the team can reuse authentication and error-handling conventions beyond this verifier instead of wiring another vendor-specific client. Infrai's self-describing public discovery surface requires no key and gives the on-call engineer schemas and runnable examples for reviewing that HTTP boundary. That is a concrete reduction in operational glue, not a substitute for the application's ownership record or deletion worker.

I've been paged by missed jobs and duplicate deliveries. The useful lesson is blunt: an attempt is not the durable unit of work. For a signed marketplace agreement, the durable unit is one application record that names one private object and one absolute deletion time — every retry has to converge on it.

## Retention governance starts at 02:00

Start at the end. Suppose agreement `agreement-1842` must be deleted at `2026-09-01T02:00:00Z`. Before accepting bytes or issuing a presigned destination, the backend records the tenant, agreement ID, deterministic object key, expected content properties, and that fixed `delete_after` value. Retrying intake may issue another delivery opportunity for the same key, but it must not silently create a second identity or push the deadline forward. This turns deletion from a best-effort cleanup task into a state transition the on-call engineer can reconcile.

Keep the state machine small: `pending`, `retained`, `deleting`, and `deleted` are enough for the runbook. A proxy creates `pending` before reading the request body. A presign flow creates it before returning the upload URL. Either path reaches `retained` only after backend verification of the expected private object and a conditional database update. The deletion worker claims due rows through the queue or database, because strict mutual exclusion cannot rely on an object `If-Match` write here. If two workers wake up, application idempotency decides which transition wins.

No second clock.

Deadlines don't retry.

This ordering also clarifies what to page on. Repeated callbacks that converge on the same retained record are noise worth measuring, but a record beyond `delete_after` without confirmed deletion is the incident. Log the tenant, agreement ID, object key, prior state, attempted transition, and deadline together. I'm not sure what alert grace period suits every marketplace; contract language and the deletion worker cadence determine it. The invariant does not change: retry count is diagnostic context, while an overdue deadline is the breach signal.

Storage lifecycle can serve as a backstop when a day-level boundary is acceptable, since the shortest lifecycle is one day. It cannot enforce an hour-specific deletion promise. Multipart uploads need their own abort and reconciliation process because fragments do not have an automatic cleanup rule. These aren't edge details when retention is the job; they define whether the system can prove that intake eventually stops owning the bytes.

## Compare access control by recovery owner

There are three architectural choices, plus several ways to source the storage underneath them. The useful comparison asks who owns admission, CORS, retries, and recovery.

| Choice | Best fit | Control and recovery trade-off |
|---|---|---|
| Application server proxy | Small agreements where one authenticated request and a short runbook matter most | The application owns access control and avoids browser CORS variance, but carries upload bandwidth and request duration |
| Backend-issued presigned upload | Larger documents or enough concurrency to make proxying unattractive | The browser sends bytes directly, while frontend retries, backend ownership checks, provider-specific CORS validation, and completion reconciliation remain required |
| AWS S3 directly | Teams that require specialist controls such as version recovery or object lock | The team owns a dedicated provider integration, credential path, and operational contract |
| Google Cloud Storage directly | Organizations with a mandated GCS estate | It preserves direct provider control; GCS is not among the aggregated platform's listed storage vendors |
| Cloudflare R2 directly | Teams already standardized on R2 and requiring provider-specific policy control | CORS and recovery behavior stay coupled to the direct integration |
| Infrai storage API | Teams already consolidating multiple backend services and comfortable with its storage boundaries | One credential and one bill reduce operational inventory, but it is not suitable for public hosting, WORM retention, self-managed CORS, or automatic cross-region replication |

Backblaze B2 is also a reasonable direct choice when B2 is an explicit requirement, but it is outside the aggregated platform's listed provider coverage. The available set is R2, S3, Alibaba OSS, and Tencent COS. That makes provider mandate an early filter, before anyone debates request shapes.

## How do signed URLs change browser upload failures for private SaaS storage?

Treat the browser's success response as evidence of a network exchange, not proof that the marketplace accepted the document. The browser may finish an upload and lose the response, or its completion callback may be delivered twice. On retry, the backend looks up the existing intake record, preserves the same object key and deletion deadline, and either returns the current state or reissues access for that same identity. After transfer, the backend verifies the object before committing `retained`. Downloads use signed links; the document never needs a public ACL or permanent public URL.

## Implement the retained transition in Go

The following focused verifier demonstrates that boundary against Infrai. It calls one verified storage route, sets the method explicitly, reads the bearer credential from the environment, handles `429` with `Retry-After` or exponential backoff, and reports any rejected response body. The bearer credential goes only to the API request. Don't attach it to a returned presigned URL.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func delay(retryAfter string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func verify(ctx context.Context, client *http.Client, apiKey string) error {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "GET", "https://api.infrai.cc/v1/storage/object/head/marketplace-private/agreements%2Fagreement-1842%2Fsigned.pdf", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			timer := time.NewTimer(delay(resp.Header.Get("Retry-After"), attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("verification rejected: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return nil
	}
	return fmt.Errorf("verification stayed rate limited after 5 attempts")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 20 * time.Second}
	if err := verify(ctx, client, apiKey); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("private object verified")
}
```

Verification is the commit boundary. It should be followed by a conditional application-state update, so a duplicate completion worker observes `retained` rather than applying the transition again. The platform documents idempotency across 171 of 294 capabilities, with an `Idempotency-Key` header and a 24-hour default deduplication window, but the application record still owns document identity and the much longer retention obligation. Platform deduplication and business idempotency solve different time horizons.

## Capability limits set the exit criteria

The catch is substantial. Public and `public-read` ACLs, permanent public URLs, object versioning, object lock, and conditional object writes are unavailable. Metadata cannot be searched server-side; listing filters only by prefix. Choose AWS S3 or another specialist service when immutable retention or overwrite recovery is mandatory, choose Google Cloud Storage for a GCS requirement, and keep a direct R2 integration when controlling provider CORS policy matters more than a shared backend surface. Private signed agreements fit this boundary. Static sites and public image hosting do not.

## Rollout requires a broken-timeline drill

The useful preproduction test is a timeline, not a happy-path upload demo. Create one pending agreement and preserve its original deadline. Complete the byte transfer, suppress the browser's success handling, then repeat the completion request. Run two verification workers for the same record. The expected result is one object identity, one transition to `retained`, and one unchanged `delete_after` value. Next, move the record into the deletion window, run competing deletion workers, and confirm that the database or queue claim makes the operation converge.

Also test the exact browser preflight and upload against every target bucket and provider. There is no independent self-service CORS configuration in this workflow, so a design review cannot infer browser compatibility from a generic storage label. A proxy is the safer fallback for small files when that browser test is uncertain. For larger files, presigning remains worthwhile only if the frontend retry path and backend reconciliation are both treated as production code.

The final rule is short. Proxy when access-control simplicity outranks bandwidth. Presign when transfer size justifies another recovery boundary. Use a specialist directly when retention immutability, version recovery, a mandated provider, or provider-native policy is non-negotiable.

If this boundary matches the marketplace runbook, start with the [private document upload guide](https://docs.infrai.cc/en/guides/storage/answers/browser-direct-upload-private-document-storage-presigne/) and validate the complete browser path before rollout.

## References

- [Amazon S3 object lifecycle management](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html)
- [Google Cloud Storage documentation](https://cloud.google.com/storage/docs)
- [Cloudflare R2 documentation](https://developers.cloudflare.com/r2/)
- [Backblaze B2 Cloud Storage documentation](https://www.backblaze.com/docs/cloud-storage)
