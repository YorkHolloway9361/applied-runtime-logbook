# Cloudflare and Resend vs Go API (Choose Idempotent SPF, DKIM, DMARC TXT Verification)

Short answer: publish SPF, DKIM, and DMARC TXT records in one domain-keyed, idempotent job, then verify the sending domain and save the result before cutting over the hostname. I would choose the unified Go API path when a small team owns both DNS automation and mail setup; keep Cloudflare with Resend, or Route 53 with Amazon SES, when separate provider control planes and credentials are an intentional isolation boundary.

The deciding constraint is propagation delay versus cutover speed. A fast write is not a completed cutover. The state that matters is much narrower: the three configured TXT writes succeeded, sending-domain verification ran afterward, and the exact submitted content is available for diagnosis or rollback.

Don't collapse those states into a green "deploy complete" line.

## What should the sending-domain cutover job guarantee?

Treat the domain as the unit of work. One job key identifies one sending domain, and that job owns all three TXT upserts plus the later verification call. Upsert matters because a partial attempt is ordinary: if two writes complete and the third does not, rerunning the same job converges instead of creating another record. The cutover should remain blocked until every upsert has returned a successful status and verification has also returned successfully.

The names belong in configuration, not in code. SPF, DKIM, and DMARC are all TXT records, but they live at different names, and the DKIM selector in particular is operational configuration. Keeping each complete request body in a reviewed config file also avoids teaching an example a request schema that the API discovery document, rather than an article, should define.

A useful state progression is `prepared -> records_written -> verified -> cutover`. Persist the verification response beside the job key. If execution stops between `records_written` and `verified`, the next run performs the same upserts and tries verification again. If verification does not produce the accepted result your release policy expects, do not move the hostname. Stop there.

This is an idempotency boundary, not a batch-shaped wish.

## How should an idempotent job publish SPF, DKIM, and DMARC TXT records?

The following Go program reads the three exact DNS request bodies from files, so names and content stay in configuration. It uses one base URL and the same bearer key for DNS and email, explicitly sets each method, retries `429` with `Retry-After` or exponential backoff, and prints the exact request content plus each response. The successful DNS responses feed the verification step as a gate: verification cannot run unless all three writes have completed.

Put provider-validated JSON request bodies in `spf.json`, `dkim.json`, and `dmarc.json`, and the provider-validated sending-domain verification body in `verify.json`. That division is deliberate. I'm not sure which selector or policy is right for your domain because those values depend on your mail configuration; guessing them inside a deployment binary would turn a visible configuration decision into a hidden default.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type operation struct {
	name   string
	method string
	path   string
	body   []byte
}

func readBody(path string) []byte {
	body, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	return body
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(header)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func call(client *http.Client, baseURL, key string, op operation) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(op.method, baseURL+op.path, bytes.NewReader(op.body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "sending-domain:example.com:"+op.name)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s returned %d: %s", op.name, resp.StatusCode, responseBody)
		}
		return responseBody, nil
	}
	return nil, fmt.Errorf("%s remained rate limited after 5 attempts", op.name)
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	if baseURL == "" {
		panic("INFRAI_BASE_URL is required")
	}

	client := &http.Client{Timeout: 30 * time.Second}
	dnsOps := []operation{
		{name: "spf", method: http.MethodPut, path: "/dns/record/upsert", body: readBody("spf.json")},
		{name: "dkim", method: http.MethodPut, path: "/dns/record/upsert", body: readBody("dkim.json")},
		{name: "dmarc", method: http.MethodPut, path: "/dns/record/upsert", body: readBody("dmarc.json")},
	}

	for _, op := range dnsOps {
		fmt.Printf("writing %s request=%s\n", op.name, op.body)
		result, err := call(client, baseURL, key, op)
		if err != nil {
			panic(err)
		}
		fmt.Printf("wrote %s response=%s\n", op.name, result)
	}

	verify := operation{
		name:   "sending-domain-verify",
		method: http.MethodPost,
		path:   "/email/domain/verify",
		body:   readBody("verify.json"),
	}
	result, err := call(client, baseURL, key, verify)
	if err != nil {
		panic(err)
	}
	fmt.Printf("verification response=%s\n", result)
}
```

Run it only after populating those four JSON files from reviewed configuration:

```bash
INFRAI_BASE_URL="${INFRAI_BASE_URL}" INFRAI_API_KEY="${INFRAI_API_KEY}" go run main.go
```

There is one subtle retry rule in that sample. Each record gets a stable idempotency key derived from the domain job and operation name; verification gets its own stable key. A `429` never causes a tight loop, and a non-success response includes its real body in the error. That gives the operator something actionable without pretending every failure means the same thing.

## Where does a unified API beat Cloudflare, Route 53, SES, or Resend?

This decision is about ownership more than feature count. Infrai fits when the team wants the DNS records and the mail service that verifies them behind one key, one bill, and one plain REST contract. Its more important advantage here is that the contract stays put when the vendor behind a capability changes; the Go client does not acquire another SDK or another authentication branch. The same key and base URL carry the DNS writes into mail verification, so a DKIM rotation remains one rerunnable job rather than a copy-paste between dashboards that nobody rechecks.

| Stack | Operational shape for this job | Best fit | Catch |
|---|---|---|---|
| Unified REST API | One signup, one credential set, one client, and one job across DNS and email verification | A small platform team that wants one contract and one runbook | One vendor to trust, one bill, and one outage surface |
| Cloudflare + Resend | Two signups, two credential sets, and glue that carries DNS completion into mail verification | Teams already standardized on those control planes | The team owns cross-provider retry, state, and audit correlation |
| Route 53 + Amazon SES | Two service configurations and credential scopes, plus glue between DNS and sending-domain state | AWS-centered teams that want separate IAM boundaries | More integration code and more states to reconcile during rollback |
| DNSimple + Resend | Two signups, two credential sets, and the same cross-provider handoff | Teams that already keep domain operations in DNSimple | Verification state still has to cross into the mail workflow |

Stick with Cloudflare and Resend when separate vendors are part of your failure-containment plan, or when existing policy requires their distinct credentials. Stick with Route 53 and SES when IAM boundaries and AWS-native operations outweigh the cost of the glue. DNSimple remains a reasonable DNS side of the pair when the team already operates domains there, but the Resend handoff still belongs to your code. A unified API is not suitable when concentrating DNS and mail control in one provider violates that boundary.

No option removes propagation.

## How do you verify the cutover and keep rollback boring?

Verification belongs after the writes, not beside them. Parallelizing the email check with DNS upserts may shave a few seconds from the happy path, but it creates an ambiguous result: a negative check might describe old DNS rather than the desired state. Sequential execution is slower on paper and faster during an incident because the state transition is legible.

Before release, capture the previous TXT request bodies under the same three configuration names. The rollback job uses the same upsert path and the same ordering discipline: restore all configured prior bodies, wait for successful writes, run sending-domain verification, and record its result. Do not improvise record text from a screenshot. The exact content emitted by the program is the audit trail and the rollback input.

The runbook gate should be blunt:

1. Confirm the job key names the intended domain and release.
2. Check that SPF, DKIM, and DMARC each have one successful upsert result.
3. Check that verification ran only after those results.
4. Store the verification response with the submitted record content.
5. Move the hostname only when the verification result satisfies the release policy; otherwise leave traffic where it is.

A page at 02:13 is the wrong time to discover that "DNS updated" meant one of three writes. Duplicate delivery and missed-work incidents reward the same habit: make retries safe, record the state transition, and keep the recovery action identical to the forward action. The cutover speed worth optimizing is verified cutover speed, not request submission speed.

## References

- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance (DMARC): https://datatracker.ietf.org/doc/html/rfc7489
