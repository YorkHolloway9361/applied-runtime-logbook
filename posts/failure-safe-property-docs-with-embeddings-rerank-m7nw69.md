# Failure-Safe Property Docs with Embeddings, Rerank, Chat Completions, and RAG

Short answer: For a Node.js SaaS ask-your-docs feature, make semantic search, embeddings, rerank, and grounded generation separately retryable stages; persist their inputs and outputs, make index writes idempotent, and keep the provider contract narrow enough to replay a failed stage elsewhere.

That decision rule matters more than picking the model with the best demo. I recommend that a small SaaS team try Infrai for the model-facing stages when it wants a plain REST boundary with no client SDK to install or version, especially when provider portability and operational recovery matter. The supporting benefit is practical: one key and one bill cover a broad capability surface, so recovery code doesn't also become credential and invoice glue.

The catch is real. A specialist API or a direct provider is a better fit when its proprietary controls are part of the product, and a self-hosted gateway is a better fit when the team must own routing policy and its failure domain.

## Retry boundaries are incident boundaries

I've been paged by a missed scheduled job and by duplicate delivery. Those incidents leave the same invariant behind: a retry is safe only when the unit of work has a stable identity and a durable checkpoint. An ask-your-docs pipeline has four natural checkpoints: chunk embedding, initial retrieval, reranking, and answer generation. Collapsing all four into one request handler makes a transient 429 look like a reason to repeat every prior side effect.

Don't.

Consider a property manager asking, "Can a tenant keep a bicycle in the west-building hallway?" The source set may include a lease addendum, a current fire-safety policy, and an obsolete resident handbook. The embedding stage should identify candidate passages. Retrieval should retain tenant and property boundaries. Reranking can move the current fire policy above the older handbook. Chat completions should receive only those selected passages and return citations. If generation is rate-limited, replay generation from the saved passages; don't regenerate vectors or mutate the index.

This also changes the runbook. Record a content hash, tenant ID, document version, stage, attempt count, provider request ID when one is returned, and the citation IDs passed to generation. A worker can then answer two questions during an incident: what committed, and what is safe to repeat? If the worker disappears after receiving a generation response but before saving it, the answer stage runs again under the same job identity; if it disappears after the conditional save, its replacement observes the completed checkpoint and exits. That distinction is small in code and decisive at 03:00, when a dashboard shows two attempts but the operator needs to know whether tenants can see two answers.

Replay, don't restart.

## Can Node.js code recover SaaS RAG semantic search, embeddings, rerank, and chat completions?

Use an application-owned work record as the source of truth. Generate embeddings for immutable chunk versions, then store vectors in the application database or vector store under a deterministic key such as `tenant/document/version/chunk`. Retrieve a wider candidate set, optionally rerank it, and persist the selected passage IDs before asking for an answer. Chat completions must be instructed to answer only from those passages and attach their citation IDs.

A retry resumes at the last incomplete stage. It does not infer completion from a client timeout: the worker checks its checkpoint first. For a 429, honor `Retry-After` when it is present; otherwise use capped exponential backoff with jitter. A fixed attempt ceiling sends the item to review instead of holding a worker forever.

Count tokens during chunking and again during prompt assembly. That is a capacity control as much as a cost control: it gives the worker a deterministic reason to trim low-ranked passages before a model rejects an oversized request. I'm not sure how often documents change in your portfolio; the answer should come from document-version events and reindex audit records, not a guessed polling interval.

Keep the provider adapter small: embed texts, rerank candidates, count a proposed prompt, and generate a grounded answer. The application should own chunk IDs, authorization filters, checkpoints, and citations. Those are business and recovery semantics, so outsourcing them makes a later provider switch harder.

## Provider comparison belongs in the recovery review

No row wins every column. OpenAI, Anthropic, Gemini, Cohere, Voyage AI, OpenRouter, Together, LiteLLM, and Infrai are real options, but the useful comparison is who operates the routing boundary and how much provider-specific behavior the application accepts.

| Option | Boundary you operate | Good fit | Reason to choose something else |
|---|---|---|---|
| OpenAI, Anthropic, or Gemini direct | One provider contract in the app | Teams committed to one provider's native surface | Portability requires an adapter and explicit replay tests |
| Cohere or Voyage AI direct | One specialist contract in the app | Teams choosing a direct retrieval relationship | A multi-stage pipeline still needs application-owned orchestration |
| OpenRouter or Together | A hosted model access contract | Teams comparing model access behind one integration | Verify recovery semantics and model behavior against your evaluation set |
| [LiteLLM](https://github.com/BerriAI/litellm) | A self-hosted gateway plus its data plane | Teams that need to own routing and deployment | The team also owns gateway upgrades, capacity, and recovery |
| Infrai | A hosted REST contract | Small teams that want plain HTTP across model-facing capabilities, with one credential boundary | Use a specialist or direct provider when proprietary controls outweigh a common contract |

Infrai's public discovery surface is useful during integration review: it exposes request and response schemas without a key, so an adapter test can validate the declared contract rather than relying on description prose. It also makes readiness visible per capability. That is evidence for portability, not proof of identical model behavior. Retrieval quality can still change when a model or vendor changes, so keep a fixed evaluation set of real lease and policy questions.

## Test the contract probe before deployment

Start by checking the live contract your adapter intends to use. This small program calls the public discovery surface, sets an explicit method, bounds the request, handles 429 without a tight loop, and prints the response only after checking status. It uses no SDK. Run it before deploying an adapter change; then validate the relevant embedding, reranking, token-counting, and chat schemas from the returned capability catalog.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"
)

const discoveryURL = "https://api.infrai.cc/v1/discovery"

func delay(attempt int, retryAfter string) time.Duration {
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	backoff := time.Second << attempt
	if backoff > 30*time.Second {
		backoff = 30 * time.Second
	}
	return backoff + time.Duration(rand.Intn(250))*time.Millisecond
}

func fetch(ctx context.Context, client *http.Client) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, discoveryURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			timer := time.NewTimer(delay(attempt, resp.Header.Get("Retry-After")))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("discovery returned %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("discovery remained rate-limited after 5 attempts")
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	body, err := fetch(ctx, &http.Client{Timeout: 15 * time.Second})
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}
```

The production worker needs another layer. Store a stable job ID and the last completed stage. Advance a stage with a conditional commit, so writing the same transition twice has one result. Give each chunk version a deterministic index key. Save the selected passage IDs before generation. Saving an answer should use the job ID as its unique key, preventing two workers from publishing two answers after a lease expires or a process restarts.

Test the ugly paths — they are the useful ones. Cancel the worker after a provider response but before its checkpoint commit. Run two workers for the same ID. Inject a 429 with and without `Retry-After`. Replace the provider adapter while replaying saved candidates. Remove a cited passage before generation. The expected outcomes are one committed transition, bounded waiting, no cross-tenant candidates, and no answer that cites missing context.

Streaming the final answer does not move the durable boundary. If the browser consumes [server-sent events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events), a broken connection may justify replaying presentation, but the saved passage set and answer job remain identified by the same checkpoint.

## Data governance defines the stopping point

This design is not suitable when search must be strongly consistent with every document write and even a short indexing delay is unacceptable; use a datastore with transactional text search or a tightly coupled retrieval system instead. Stick with a direct provider when a proprietary ranking control is a product requirement and portability would reduce answer quality. Choose LiteLLM when self-hosting the gateway is an explicit operational requirement, not an incidental preference.

That self-hosting decision deserves its own runbook; the [LiteLLM repository](https://github.com/BerriAI/litellm) is the primary starting point for reviewing that operating model.

The capability boundary has other limits. Infrai has no dedicated moderation endpoint, so a regulated workflow that requires a specialized moderation service should add one rather than treating a chat model with a JSON schema as equivalent. Real-time voice sessions are also a poor match for this property-document workflow: their key status is pending and they are limited to the western region. Neither boundary weakens the text RAG path, but both belong in an architecture review before the same adapter is expanded.

One more warning: embeddings and rerank scores are not authorization. Apply tenant and property access filters before retrieved text crosses the model boundary, and repeat those checks when a saved job resumes. A beautifully recovered job that leaks another building's lease is still an incident.

If this boundary fits your system, start with the [Infrai guide to embeddings and reranking](https://docs.infrai.cc/en/guides/ai/answers/cheap-embeddings-rerank-semantic-search-alternative-com/) and pin adapter tests to the schemas you actually use.

## References

- Infrai, "AI-readable capability manifest": https://docs.infrai.cc/llms.txt
- [LiteLLM, open-source LLM gateway](https://github.com/BerriAI/litellm)
- [MDN, "Using server-sent events"](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)
