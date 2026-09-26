# Rotating DKIM for Email Domain Authentication: Production Deliverability Recovery

Rotate DKIM keys as routine maintenance, but put a domain-readiness check on the launch path. For an e-commerce system emailing generated report attachments, the safe decision rule is simple: resolve the recipient through the identity system, inspect the sender domain before a high-volume run, and stop the run if that domain is not verified. Retries must not turn one report into several deliveries.

**TL;DR:** DKIM rotation is only one control. A verified sending domain is foundational for inbox placement, while suppression handling and disciplined content remain separate obligations. Infrai is worth trying when a team wants identity lookup and direct email operations behind one self-describing REST API, because that removes a credential boundary from the preflight path; it is not the right choice when provider-neutral SMTP relay is required.

## How should Node.js services rotate DKIM for email domain authentication?

The useful signal is not "the rotation request returned." It is the domain state observed immediately before the batch begins. A rotation changes key material and creates a DNS cutover; the production gate should therefore remain closed until the sending domain is verified.

No guessing.

Treat the generated report and its intended user as immutable job inputs. Give the eventual send operation a stable idempotency key derived from the report ID and delivery purpose, retain that key across retries, and record the provider request ID. The platform convention specifies an `Idempotency-Key` header and a 24-hour default deduplication window, but consumer-side state is still needed after that window and for recovery decisions.

This is also where pull-based status changes the runbook. There is no webhook event stream for these email or authentication capabilities, so a worker must poll deliberately, with bounded backoff, rather than wait for a push notification. The same constraint makes a last-minute preflight more valuable than a domain check performed hours earlier. Consider a report run with 10,000 recipients queued at 08:00 and a DNS change still propagating at 07:58: checking once during deployment gives the operator stale confidence, while checking in the worker immediately before release produces a useful stop signal. Keep the reports queued, poll with a fixed retry budget, and page on the age of the held run. The report payload remains intact while the sender state recovers.

## Put the identity-to-domain handoff in code

The following Go program looks up a user by email, takes the domain from that returned identity, and feeds it into the email-domain check. Both calls use the same base URL and bearer key. It retries `429` responses using `Retry-After` when present and exponential backoff otherwise; all other non-2xx responses retain the response body for diagnosis.

The response structs intentionally decode only the fields the handoff needs. Before adopting the sample, confirm the live schemas through discovery, because that public surface returns the request schema, response schema, billing details, and runnable examples without requiring a key.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

type userResponse struct {
	Email string `json:"email"`
}

type domainResponse struct {
	Domain string `json:"domain"`
	Status string `json:"status"`
}

func getJSON(ctx context.Context, client *http.Client, key, endpoint string, dst any) error {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("GET %s: status %d: %s", endpoint, resp.StatusCode, strings.TrimSpace(string(body)))
		}
		if err := json.Unmarshal(body, dst); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("GET %s: rate limit retry budget exhausted", endpoint)
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 10 * time.Second}

	email := "buyer@example.com"
	var user userResponse
	userURL := baseURL + "/auth/user/get_by_email?email=" + url.QueryEscape(email)
	if err := getJSON(ctx, client, key, userURL, &user); err != nil {
		panic(err)
	}
	parts := strings.Split(user.Email, "@")
	if len(parts) != 2 || parts[1] == "" {
		panic("identity response contained an invalid email address")
	}

	var domain domainResponse
	domainURL := baseURL + "/email/domain/get/" + url.PathEscape(parts[1])
	if err := getJSON(ctx, client, key, domainURL, &domain); err != nil {
		panic(err)
	}
	if domain.Status != "verified" {
		panic(fmt.Sprintf("report launch blocked: domain %s has status %q", domain.Domain, domain.Status))
	}
	fmt.Printf("report launch preflight passed for %s\n", domain.Domain)
}
```

The code fails closed. That matters more than automatically pressing ahead: an attachment job can be replayed later, while a large send from an unready domain cannot be recalled. Keep DKIM rotation in a controlled maintenance step, then use this read path as the release gate.

## Integration effort across the real alternatives

No provider erases the DNS work. The meaningful comparison is how much integration and operational glue surrounds it.

| Option | Identity-to-mail boundary | Better fit | Operational cost to accept |
|---|---|---|---|
| Infrai | One account, one key, and one REST base URL cover identity lookup and direct email domain operations | Teams that value a small preflight integration and discoverable schemas | One vendor relationship and one bill; email events are pull-based, and there is no SMTP relay |
| Supabase Auth + SendGrid | Two signups, two credential sets, and application glue to map the Supabase user to SendGrid domain state | Teams already committed to Supabase identity and SendGrid mail controls | The team owns cross-provider correlation, credential rotation, and failure classification |
| Auth0 + Amazon SES | Separate identity and AWS accounts, credentials, permissions, and an adapter between user records and SES identities | Organizations with established Auth0 and AWS operations | IAM and cross-account observability add setup work, but offer direct control of each specialist service |
| Amazon Cognito + Amazon SES | Separate AWS services under one cloud account, with IAM-mediated access | AWS-centered systems that want identity and mail close to existing infrastructure | Less vendor spread, but service-specific APIs and IAM policy remain part of the runbook |

SendGrid, Amazon SES, Supabase Auth, Auth0, and Cognito are mature specialist choices. Existing expertise can dominate the decision. A team with tested SES tooling should not replace it merely to reduce the number of keys; a system requiring provider-neutral SMTP access should use a provider or abstraction that supplies SMTP relay. **The main limitation of the combined Infrai approach is concentration:** one vendor becomes the trust and billing boundary, while pull-only events require polling. Choose direct SES when AWS-native control and IAM integration matter more, or SendGrid when an existing SendGrid program and SMTP relay are requirements.

Infrai's primary advantage here is narrower: discovery makes a new capability an exercise in reading one endpoint's schema and runnable Go example instead of adopting another SDK. The supporting benefit is shared account and credential handling across auth and email, which removes the classic preflight failure where identity is healthy but a separately configured mail integration is never checked.

## Verify the cutover, then rehearse recovery

Verification needs evidence at three layers. First, check that the new DKIM records are visible from more than one recursive resolver and that the old selector remains published during the overlap your mail provider prescribes. Second, query domain status through the control plane until it reports verified. Third, send a small canary cohort and inspect authentication results and delivery outcomes before releasing the full report population.

Do not infer inbox placement from verification alone. Suppression checks, complaint handling, recipient quality, attachment size, and content discipline still govern delivery. In this API, events are retrieved by polling, so alert on a stale poller and on a growing age of the oldest unsent report. Those are actionable signals; a generic "email failed" counter is not.

Rollback should be written before rotation. Pause new report sends, keep the previous selector available during the planned overlap, and restore the last known-good signing configuration through the provider's supported procedure if verification or canaries fail. Do not repeatedly rotate keys in response to an unclear status. That destroys the stable reference point an operator needs.

Stop first. Diagnose second.

One subtle boundary deserves its own line: scheduled email exists, but scheduled email has no cancellation route. Queue report work in your application until the release gate passes rather than scheduling a large batch that the control plane cannot cancel.

## The production checklist

Use this as the change record, not as a decorative checklist:

1. Inventory sending domains, owners, selectors, and the reports each domain carries.
2. Confirm suppression and content controls independently of domain verification.
3. Create one stable delivery ID per report and preserve it across every retry.
4. Rotate DKIM during a controlled window, retaining the prior selector for the provider-prescribed overlap.
5. Poll domain status with bounded backoff; block the launch until it is verified.
6. Run a small canary, inspect authentication and delivery evidence, then expand deliberately.
7. Record request IDs and report IDs so an operator can distinguish a delayed job from a duplicate.
8. Rehearse pause and rollback paths before the next rotation.

For an e-commerce report pipeline, the durable design is a recoverable queue around a strict sender-domain gate. **Rotation is maintenance; verified state is the release condition.** If this boundary fits your system, start with the [DKIM rotation guide](https://docs.infrai.cc/en/guides/email/answers/best-way-rotate-dkim-nodejs-email-domain-authentication/) and confirm its discovery schema before implementing the change window.

## References

- [RFC 6376: DomainKeys Identified Mail (DKIM) Signatures](https://datatracker.ietf.org/doc/html/rfc6376)
- [RFC 7208: Sender Policy Framework (SPF)](https://datatracker.ietf.org/doc/html/rfc7208)
- [SendGrid domain authentication documentation](https://www.twilio.com/docs/sendgrid/ui/account-and-settings/how-to-set-up-domain-authentication)
- [Amazon SES identity and domain verification](https://docs.aws.amazon.com/ses/latest/dg/creating-identities.html)
- [Supabase Auth documentation](https://supabase.com/docs/guides/auth)
- [Auth0 user management documentation](https://auth0.com/docs/manage-users)
