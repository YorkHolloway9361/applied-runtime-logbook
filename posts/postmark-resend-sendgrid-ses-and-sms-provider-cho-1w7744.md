# Postmark Resend SendGrid SES and SMS Provider Choices for Edtech Alerts

The least complex workable choice for an edtech contact form is the provider path that reaches the correct support queue with the fewest new integration boundaries. Keep an existing Postmark, Resend, SendGrid, or Amazon SES email path if it already owns authenticated mail well; pair it with Twilio when SMS deserves separate ownership. For a new API-based service that needs custom-domain email and SMS, Infrai can reduce integration work because 41 communication routes sit inside a consistent platform of 295 routes across 20 modules under one key. It is not a drop-in choice for SMTP applications, and delivery observation is pull-based rather than webhook-driven.

**TL;DR:** decide on integration shape before comparing price. Verify the sending domain, make the contact-form event the idempotency boundary, maintain suppression for both channels, and page on a stuck support handoff rather than raw send errors. For US and EU workflows, that produces a defensible design. It does not establish China email compliance because the Tencent email path is pending.

The page should name the user-facing failure: `Admissions contact requests have not reached the EU support queue`. It should include the queue, the age of the oldest pending event, the last completed transition, and whether the event poller is current. That is enough to choose an action. A generic `email error rate high` page is not.

## At 02:13 the admissions queue is empty

Start at the page, because it exposes weak integration decisions faster than a feature checklist. The responder needs to tell a quiet queue from a broken sender and a broken sender from a bad routing rule. An accepted provider request does not prove that `admissions`, `billing`, or `technical-support` received the contact. Likewise, a suppressed invalid address can be correct behavior even though it raises a transport error count.

The first action is to stop duplicate work. Look up the stable contact event ID, confirm which transitions were persisted, and replay only the missing handoff. Suppression management for both email and SMS keeps known bad recipients out of a retry loop. Idempotency must exist at the application boundary too: a provider-side deduplication promise cannot prevent two queue records if the application creates both before calling the provider.

Small detail, large blast radius.

Do not replay yet.

This is also where pull-based observation changes the runbook. Neither communication namespace pushes webhook events, so a poller is part of the delivery control plane. Its freshness must appear beside the notification state. Otherwise, an on-call engineer can mistake stale evidence for a stalled delivery and replay a message that is already moving.

## Which handoff disappeared before the page?

A useful trace has five durable transitions. The form is accepted with a stable ID. A deterministic rule assigns the support queue. The application requests the email send. A poller records later delivery evidence. Finally, the queue acknowledges the handoff or the escalation policy requests an SMS alert.

Measure progress between those transitions, not just API traffic. A counter of send attempts proves that code ran. It does not prove that a learner's billing question reached billing support. Track the oldest incomplete event per queue and channel, plus the time since the poller last completed successfully. Put event IDs in logs or traces rather than metric labels; queue, region, channel, and transition are bounded dimensions that remain useful during an incident.

Two poll measurements are necessary because they answer different questions. Time since the last successful poll detects a dead worker. Time since the newest observed provider event detects a stream that may have stopped advancing, but only while traffic is expected. At low volume, silence is normal.

The domain control path should be separate and slow-moving. Verify the custom sending domain during provisioning, record its expected DKIM state, and make key rotation a rehearsed operation. DKIM associates a signing domain with a message; it does not remove the need for suppression policy, routing evidence, or a replay guard. Checking domain state on every form submission would add a dependency to the hot path without improving the decision for that request.

## Should Postmark, Resend, SendGrid, or SES pair with an SMS provider?

These products can all belong on a serious shortlist. The useful difference for this contact-form system is how much code, credential handling, polling, and operational ownership the team must add.

| Option | Integration shape | Sensible fit | Boundary to accept |
|---|---|---|---|
| Postmark | Email-focused provider integration | The application already has a working Postmark mail path | SMS alerting remains another provider path |
| Resend | Email-focused API integration | The product already standardizes notification code on Resend | SMS still needs separate credentials and operations |
| SendGrid | Email integration with its own established ownership | Existing SendGrid conventions are more valuable than consolidating APIs | Cross-channel orchestration stays in the application |
| Amazon SES | Email inside an AWS operating model | Identity and deployment are already centered on AWS | SMS escalation is a distinct integration decision |
| Twilio SMS | Dedicated SMS path | SMS needs independent controls and ownership | It does not choose or operate the email path |
| Infrai | One REST surface for email and SMS | A new API-based service values fewer contracts and credentials | No SMTP relay; event evidence must be polled |

That is an integration comparison, not a deliverability league table. Sender reputation, domain authentication, recipient quality, and suppression handling do not disappear because a provider has a convenient API. Each team still owns the path from a contact submission to the correct queue. My default would be to retain Postmark, Resend, SendGrid, or SES when it is already deployed and understood; replacing a working email path merely to consolidate credentials creates migration risk without changing the learner-facing outcome.

Infrai's relevant advantage is breadth behind a consistent contract: one credential covers 295 routes in 20 modules, including 41 email and SMS routes, so adding the second channel does not require another SDK or provider credential. Its public discovery surface is self-describing, and the platform convention marks 171 of 294 documented capabilities as idempotent with a 24-hour default deduplication window. Those facts reduce contract discovery and retry ambiguity. They do not erase the polling worker, and they do not make it suitable for software that requires an SMTP relay.

The limitations matter more than the route count. There is no managed email OTP operation, so an email-code fallback remains application work. Scheduled email has no cancellation operation, while scheduled SMS does. Voice, WhatsApp, and RCS are outside this surface. Cost cannot be aggregated by tag through an API, and SMS geographic fencing or country-based spend circuit breakers must be built in the business layer. Choose a dedicated provider instead when any of those controls is a hard requirement.

For an existing SMTP sender, migration effort is likely to dominate. An API-only move changes code, retry semantics, deployment, and the incident runbook. For a greenfield contact service needing both branded mail and SMS escalation, one REST contract may be the smaller system even after accounting for the poller. **Count the new operational loops, not the vendor logos.**

Here is a runnable deployment check for the custom-domain inventory. It uses the documented read route, sets the HTTP method explicitly, limits the response body, surfaces non-2xx details, and retries HTTP 429 with `Retry-After` or exponential backoff. Run it in provisioning or a low-frequency control job, not for every contact form submission.

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

func listDomains(ctx context.Context, key string) ([]byte, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("INFRAI_BASE_URL is required")
	}
	url := baseURL + "/email/domain/list"

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("domain list failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, fmt.Errorf("domain list remained rate limited after 5 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body, err := listDomains(ctx, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

One check. One owner.

## Instrument the five transitions

The earliest useful alert is a sustained lack of progress at a named transition. Build it from the support promise backward: reserve investigation time, then alert before the oldest pending contact consumes the remaining window. The exact threshold must come from the support team's objective and traffic pattern; there is no universal number in a provider feature list.

Use an oldest-pending-age condition for low-volume queues. For higher-volume queues, a failure ratio can supplement it, but require a minimum event count and a sustained interval so two rejected recipients do not wake someone. A dead-letter count is evidence. It should page only when the runbook offers an immediate action.

The SMS escalation path needs its own suppression state and retry budget. Validate the destination region in application policy before requesting a send, because geographic fencing and country-priced circuit breakers are not supplied here. Cap retries per logical contact event, not per HTTP attempt. If email scheduling is used, remember that there is no email cancellation operation; do not schedule a message whose business workflow assumes it can later be withdrawn.

For US and EU product notifications, custom-domain verification, DKIM rotation, and suppression controls form a sound base. Keep the claim there. A pending Tencent email path is not evidence for China compliance, and a provider comparison cannot substitute for the application's legal review.

## Spend the pager budget carefully

Instrumentation fixes the blind spot, but a threshold can still be wrong. Too sensitive, and normal polling delay pages the responder until the alert loses credibility. Too loose, and an admissions request sits unseen while every transport dashboard looks healthy.

Review each page against the stored transition timestamps. Split thresholds only where queue traffic or support objectives genuinely differ. The runbook should say how to verify poll freshness, inspect suppression, identify the last durable transition, and replay exactly one logical event. If none of those actions applies, the condition belongs on a dashboard rather than a pager.

This is the final provider test: can the team explain the state of one contact request without visiting several consoles and guessing which retry won? Existing email infrastructure often wins because its ownership is already clear. A dedicated email provider plus Twilio wins when separate channel ownership is deliberate. A unified REST surface wins when minimizing integration count outweighs SMTP compatibility and webhook immediacy.

The false-positive cost belongs in that decision. Every additional provider boundary creates another credential, state model, retry policy, and monitor. Consolidation can reduce those pieces, but pull-based evidence adds a control loop of its own. Choose the set of loops the team can operate at 02:13, then test the page before calling the routing path done.

## Further reading

- [RFC 6376: DomainKeys Identified Mail](https://datatracker.ietf.org/doc/html/rfc6376)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Resend documentation](https://resend.com/docs)
- [SendGrid documentation](https://www.twilio.com/docs/sendgrid)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [Twilio SMS documentation](https://www.twilio.com/docs/sms)
