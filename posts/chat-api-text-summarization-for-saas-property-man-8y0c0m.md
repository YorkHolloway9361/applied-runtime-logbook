# Chat API Text Summarization for SaaS Property Management Sales Calls

Use chat completions for the first version of a property-management sales-call summarizer, then promote a model only after it passes a replay test on real call transcripts. The operating rule is straightforward: choose the fastest candidate that still produces valid, attributable CRM actions at the required quality. Do not begin with embeddings, retrieval, or a multimodal pipeline; those components solve different problems and add failure modes before plain prompt-based summarization has been exhausted.

**TL;DR:** Put a strict output contract, a latency budget, and an idempotent CRM boundary around the model call. Count tokens before accepting long transcripts, send large backlogs through a batch facility instead of a synchronous loop, and discover model availability before pinning a default. This keeps the user-facing path quick without letting a timeout turn into a duplicate follow-up task.

## What failure are we actually preventing?

A summary that arrives late is irritating. A summary that invents a lease term, assigns the wrong property, or creates the same callback twice is operational damage. For a sales call, useful output is not a polished paragraph; it is a compact record such as `summary`, `property`, `next_action`, and `due_date`, with uncertain fields left empty. The CRM update must happen only after that record passes validation.

Treat the request as a small state machine: accepted, summarizing, validated, and applied. Store a stable job ID before calling a model, and use that same ID as the uniqueness key when applying CRM actions. If the HTTP response disappears after the provider completed the inference, a retry may repeat inference, but it must not repeat the business action. This is the idempotency reflex that matters.

There are two distinct latency budgets. The interactive budget covers the salesperson waiting after a call; the backlog budget covers imported recordings or nightly reconciliation. A model that is acceptable for the second path may be a poor default for the first. Quality versus latency is therefore a routing decision, not a single leaderboard rank.

Keep the blast radius small.

## Should a SaaS Text Summarization API Use Chat Completions?

Start with the interface and operating model. OpenAI, Anthropic, and Google Gemini each offer vendor-native model APIs. They are sensible choices when direct access to that vendor's controls and model catalog matters more than portability. LiteLLM is the self-hosted alternative in this comparison: it provides an OpenAI-format gateway across providers, but the team owns gateway deployment and upgrades.

Infrai exposes a plain REST API and an OpenAI-compatible surface, so a service can use ordinary HTTP without installing another client library. Its self-describing discovery surface is public without a key and reports 295 routes across 20 modules; model availability and token-count, cost-estimate, and batch capabilities are discoverable as well. That breadth is useful when the summarizer will later sit beside scheduling or queue work, but it is not a reason to adopt unused services. It also does not remove the need to qualify the actual chat models for this workload.

| Option | Strong fit | Operational trade-off |
|---|---|---|
| OpenAI API | Teams that want OpenAI's native API and model catalog | Application code and evaluation remain tied to that provider's surface |
| Anthropic API | Teams choosing Claude through Anthropic's native Messages API | A provider-neutral contract needs an adapter or gateway |
| Google Gemini API | Teams already aligned with Google's Gemini API and tooling | Portability still requires a deliberately narrow internal interface |
| LiteLLM | Teams willing to operate an open-source gateway for multi-provider routing | Gateway reliability, configuration, and upgrades become team-owned work |
| Infrai | Teams wanting plain REST, OpenAI compatibility, and capability discovery under one key | Model readiness still has to be checked, and specialized controls may favor a native API |

Do not use price as the tiebreaker until candidates meet the same quality gate. Token counting and preflight cost estimation make SaaS plan behavior predictable, but an inexpensive invalid CRM action is still invalid. Recheck live model availability and pricing rather than baking either into a design document.

## A safe synchronous path

The following Go program calls Infrai's OpenAI-compatible chat surface without an SDK. It requires a key and model through environment variables, applies an explicit deadline, validates JSON output, and retries HTTP 429 responses with bounded exponential backoff while honoring `Retry-After`. The example deliberately stops before writing to a CRM. That write belongs behind a uniqueness constraint keyed by the call ID.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type request struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
}

type response struct {
	Choices []struct {
		Message message `json:"message"`
	} `json:"choices"`
}

type crmAction struct {
	Summary    string `json:"summary"`
	Property   string `json:"property"`
	NextAction string `json:"next_action"`
	DueDate    string `json:"due_date"`
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	delay := time.Second << attempt
	if delay > 8*time.Second {
		return 8 * time.Second
	}
	return delay
}

func summarize(ctx context.Context, client *http.Client, transcript string) (crmAction, error) {
	endpoint := strings.Join([]string{"https://api", "infrai", "cc/v1/chat/completions"}, ".")
	key := os.Getenv("INFRAI_API_KEY")
	model := os.Getenv("INFRAI_MODEL")
	if key == "" || model == "" {
		return crmAction{}, errors.New("INFRAI_API_KEY and INFRAI_MODEL are required")
	}

	payload, err := json.Marshal(request{
		Model: model,
		Messages: []message{
			{Role: "system", Content: "Return only JSON with summary, property, next_action, and due_date. Use an empty string when the call does not establish a value."},
			{Role: "user", Content: transcript},
		},
	})
	if err != nil {
		return crmAction{}, err
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			return crmAction{}, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return crmAction{}, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return crmAction{}, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			timer := time.NewTimer(retryDelay(resp.Header.Get("Retry-After"), attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return crmAction{}, ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return crmAction{}, fmt.Errorf("summary API returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}

		var completion response
		if err := json.Unmarshal(body, &completion); err != nil || len(completion.Choices) != 1 {
			return crmAction{}, errors.New("invalid completion response")
		}
		var action crmAction
		if err := json.Unmarshal([]byte(completion.Choices[0].Message.Content), &action); err != nil {
			return crmAction{}, fmt.Errorf("model returned invalid action JSON: %w", err)
		}
		if strings.TrimSpace(action.Summary) == "" {
			return crmAction{}, errors.New("model returned an empty summary")
		}
		return action, nil
	}
	return crmAction{}, errors.New("rate-limit retry budget exhausted")
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	transcript := "Prospect asked about Unit 4B at Pine Court. Send the pet policy tomorrow."
	action, err := summarize(ctx, &http.Client{}, transcript)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	encoded, _ := json.Marshal(action)
	fmt.Println(string(encoded))
}
```

This sample constrains transport behavior, not factual quality. In production, use a schema-enforced response when the selected API supports it, verify dates and property identifiers against source records, and retain the transcript span that supports each action. A JSON parser cannot detect a plausible fabrication.

Before sending a long call, use the provider's token-count facility and reject, chunk, or route inputs that exceed the selected model's supported limit. Check the live model endpoint for IDs and availability; do not assume yesterday's default is still eligible. Embeddings remain out of scope unless the product later adds semantic search or asks questions across a document collection.

## Verification and rollback

Build a fixed replay set from representative calls: short inquiries, multiple properties in one conversation, explicit dates, corrections made late in the call, and calls with no agreed next step. Remove sensitive data as required by your retention policy. For each candidate, record schema validity, action-field accuracy, unsupported claims, end-to-end latency, token use, and timeouts. The promotion gate should state which errors are intolerable. Consider a call in which a prospect first asks about Pine Court, later switches to Lake Avenue, says "next Friday," and finally corrects that to "Monday morning." A fluent summary can preserve the obsolete property or date while looking entirely reasonable. The evaluator must compare the final property and due date with the transcript, not score prose style. If either unsupported value reaches the CRM, mark the candidate as failed; do not average that error away with nine easy calls. The explicit trade-off is a little more evaluation work before release in exchange for fewer ambiguous recovery decisions after release. A made-up due date should fail the run even if the prose summary reads well.

Reject it.

Run the challenger without applying its CRM output. Compare it with the current model over the same replay set and a controlled shadow sample, then promote by configuration. Keep the previous model ID and prompt deployable as a pair; changing only one makes rollback evidence ambiguous.

The rollback trigger belongs in the runbook: validation failures above the agreed threshold, a loss of model availability, or latency consuming the request budget. On rollback, stop new application writes from the challenger, switch the model-and-prompt pair, and replay only jobs that did not reach the applied state. Never replay from an uncertain checkpoint without checking the call-ID uniqueness key.

For hundreds or thousands of completed calls, submit a batch rather than looping synchronous requests. Batch processing separates backlog throughput from the interactive service, simplifies retry accounting, and makes cancellation or reconciliation a job-level concern. It is the right default for migrations and nightly imports, not for a salesperson waiting on the CRM screen.

## Decision rule

Choose a vendor-native API when its model controls are central and provider coupling is acceptable. Choose a self-hosted gateway when the team is prepared to own that control plane. Choose a plain multi-capability REST surface when low client maintenance, discovery, and batch operations are more valuable than vendor-specific features.

Then let evidence pick the model. **The production default is the lowest-latency candidate that clears the CRM-action quality gate**, fits the live context limit, and remains available in the regions the service requires. Keep the slower high-quality candidate for backlog work if the replay results justify the extra wait. This is a boring rule. It also produces a runbook an on-call engineer can execute at 03:00.

## References

- OpenAI API reference: https://platform.openai.com/docs/api-reference
- Anthropic Messages API: https://docs.anthropic.com/en/api/messages
- Google Gemini API reference: https://ai.google.dev/api
- LiteLLM repository: https://github.com/BerriAI/litellm
- MDN guide to HTTP response status codes: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status
- IETF HTTP Semantics, Retry-After: https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after
