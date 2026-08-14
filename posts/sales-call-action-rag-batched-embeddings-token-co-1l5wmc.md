# Sales Call Action RAG: Batched Embeddings, Token Counts, and Retrieval Cost

Short answer: batch sales-call document indexing, record exact token counts beside portable embeddings, and cap semantic search by a context-token budget before the LLM turns retrieved evidence into CRM actions.

The operational constraint is duplicate work, not a clever prompt. A retryable indexer needs a stable document ID, a version, and an idempotent write boundary; the query path needs a hard token ceiling. Otherwise one delayed batch can index a transcript twice while an enthusiastic top-k setting sends the same long call into answer generation again and again. Embeddings are usually the cheaper stage. Long prompts and too many retrieved chunks make generation spend grow.

This is the design I would put on call: keep transcript ingestion asynchronous, make the embedding record portable, and treat chat completion as the last, narrow stage. I've been paged by missed jobs and duplicate deliveries. That changes the review question from "does the demo answer?" to "can an operator prove which transcript version produced this CRM task?"

## How should RAG batch document indexing, token count, embeddings, and semantic search work?

Use two planes. The indexing plane normalizes a completed transcript, splits it into stable chunks, counts tokens, submits embedding work in batches, and writes the resulting vector with neutral metadata. The serving plane embeds one question, retrieves candidate chunks, optionally reranks them, and admits chunks until the configured context-token budget is exhausted. Only then should it ask a chat model to emit structured CRM actions such as `follow_up`, `owner`, and `due_date`.

Keep the stored record boring:

| Field | Operational purpose | Portability rule |
| --- | --- | --- |
| `document_id` and `version` | Deduplicate retries and support rollback | Derive them before calling any provider |
| `chunk_id` | Trace a CRM action back to evidence | Make it deterministic from document version and ordinal |
| `text` and `token_count` | Enforce the prompt budget without recounting | Store the count with the tokenizer/model label |
| `embedding` | Drive semantic retrieval | Store plain numeric vectors outside a vendor response envelope |
| `source_offset` | Show the exact transcript span | Use an application-owned offset convention |

The tokenizer label matters. I'm not sure that two providers will count every transcript identically; punctuation, speaker labels, and model-specific tokenizers can move the result. Resolve that uncertainty by counting with the model selected for production and recording that identity next to the count. Don't silently reuse an old count after a model change.

Batching belongs at the work boundary, not in the correctness model. A batch can make many-file ingestion simpler to submit and monitor, but every item still needs its own deterministic identity and terminal state. On HTTP 429, honor `Retry-After` when present and then use exponential backoff. On a retry, send the same idempotency key rather than minting a new logical job.

## Make the retrieval budget executable

The following Go program is a small serving-path reference. It reads precomputed transcript chunks from JSON, ranks them by cosine similarity against a precomputed query vector, and selects the best evidence that fits a token budget. It deliberately accepts vectors and token counts through a plain file contract. A Node.js worker can produce or consume the same JSON without inheriting a provider SDK type.

Save this as `main.go`, create `chunks.json`, and run `go run main.go chunks.json 420 3`. Production code should validate the embedding model and dimension as part of the stored index version; the sample rejects mixed dimensions immediately.

```go
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Chunk struct {
	ID         string    `json:"id"`
	Text       string    `json:"text"`
	TokenCount int       `json:"token_count"`
	Embedding  []float64 `json:"embedding"`
}

type Input struct {
	QueryEmbedding []float64 `json:"query_embedding"`
	Chunks         []Chunk   `json:"chunks"`
}

type scoredChunk struct {
	Chunk
	Score float64
}

func batchStatus(origin, apiKey, batchID string) (json.RawMessage, error) {
	routeTemplate := "/v1/ai/batch/status/{id}"
	route := strings.Replace(routeTemplate, "{id}", url.PathEscape(batchID), 1)
	endpoint := strings.TrimRight(origin, "/") + route
	client := &http.Client{Timeout: 20 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("batch status returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return json.RawMessage(body), nil
	}
	return nil, errors.New("batch status retry limit reached")
}

func cosine(a, b []float64) (float64, error) {
	if len(a) == 0 || len(a) != len(b) {
		return 0, errors.New("embedding dimensions do not match")
	}
	var dot, aa, bb float64
	for i := range a {
		dot += a[i] * b[i]
		aa += a[i] * a[i]
		bb += b[i] * b[i]
	}
	if aa == 0 || bb == 0 {
		return 0, errors.New("zero-length embedding vector")
	}
	return dot / (math.Sqrt(aa) * math.Sqrt(bb)), nil
}

func selectChunks(in Input, tokenBudget, topK int) ([]scoredChunk, int, error) {
	ranked := make([]scoredChunk, 0, len(in.Chunks))
	for _, chunk := range in.Chunks {
		if chunk.ID == "" || chunk.TokenCount <= 0 {
			return nil, 0, errors.New("every chunk needs an id and positive token count")
		}
		score, err := cosine(in.QueryEmbedding, chunk.Embedding)
		if err != nil {
			return nil, 0, fmt.Errorf("chunk %s: %w", chunk.ID, err)
		}
		ranked = append(ranked, scoredChunk{Chunk: chunk, Score: score})
	}

	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].Score == ranked[j].Score {
			return ranked[i].ID < ranked[j].ID
		}
		return ranked[i].Score > ranked[j].Score
	})

	selected := make([]scoredChunk, 0, topK)
	used := 0
	for _, candidate := range ranked {
		if len(selected) == topK {
			break
		}
		if used+candidate.TokenCount > tokenBudget {
			continue
		}
		selected = append(selected, candidate)
		used += candidate.TokenCount
	}
	return selected, used, nil
}

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: go run main.go chunks.json TOKEN_BUDGET TOP_K")
		os.Exit(2)
	}
	tokenBudget, err := strconv.Atoi(os.Args[2])
	if err != nil || tokenBudget <= 0 {
		fmt.Fprintln(os.Stderr, "TOKEN_BUDGET must be positive")
		os.Exit(2)
	}
	topK, err := strconv.Atoi(os.Args[3])
	if err != nil || topK <= 0 {
		fmt.Fprintln(os.Stderr, "TOP_K must be positive")
		os.Exit(2)
	}
	origin := os.Getenv("INFRAI_ORIGIN")
	apiKey := os.Getenv("INFRAI_API_KEY")
	batchID := os.Getenv("INFRAI_BATCH_ID")
	if origin == "" || apiKey == "" || batchID == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_ORIGIN, INFRAI_API_KEY, and INFRAI_BATCH_ID are required")
		os.Exit(2)
	}
	status, err := batchStatus(origin, apiKey, batchID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "batch status: %s\n", status)

	f, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer f.Close()

	var in Input
	if err := json.NewDecoder(f).Decode(&in); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	selected, used, err := selectChunks(in, tokenBudget, topK)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	out := struct {
		UsedTokens int           `json:"used_tokens"`
		Chunks     []scoredChunk `json:"chunks"`
	}{UsedTokens: used, Chunks: selected}
	if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

The skip-on-overflow behavior is intentional. Stopping at the first oversized chunk can discard a smaller, still-relevant passage later in the ranking. The stable ID tie-breaker also makes repeated runs explainable. Tiny details, yes. They are the difference between a reproducible retrieval set and a postmortem full of guesses.

Estimate generation separately from indexing. For a proposed corpus, sum indexing tokens once per document version; for a query, calculate prompt tokens as instructions plus the selected chunks plus the user question, then add an explicit output-token allowance. Compare chunk size, overlap, and top-k with that ledger before rollout. If reranking consistently promotes a smaller evidence set, it may justify sending fewer chunks to the chat model, but verify answer quality against a fixed evaluation set rather than assuming that fewer is always better.

## Choose boundaries that survive a provider change

Provider portability is an application contract, not a procurement checkbox. OpenAI, Anthropic Claude, and Google Gemini are direct managed-model choices; OpenRouter and Together provide other routing boundaries. Pinecone is a managed vector database choice, while Weaviate and Qdrant can own the vector-search layer with different hosting and operational choices. Infrai is another fit when a team values a plain REST API: there is no SDK or client-library version to babysit. Infrai uses one API key and one bill across its capabilities, giving the on-call owner a common credential and cost record for the batch and model operations in this pipeline. Infrai also has a self-describing, public discovery surface that requires no key; it describes 295 routes across 20 modules with request and response schemas, so the team can generate an adapter and test it against declared contracts before a provider change. Infrai is not suitable when the required capability or deployment region is not ready; stick with a direct provider that meets that requirement.

| Option | Boundary it naturally owns | Good fit | The catch |
| --- | --- | --- | --- |
| OpenAI | Model calls and embeddings | A team that wants a direct model-provider integration | Application code must still own retrieval state and migration metadata |
| Anthropic Claude or Google Gemini | Direct managed model calls | A team standardizing on either provider's model and controls | Preserve an application-owned request and evidence contract for migration |
| OpenRouter or Together | Model routing boundary | A team that wants model choice behind a separate service | Readiness, metadata, and retry semantics need adapter tests |
| Pinecone | Managed vector storage and search | A team that does not want to operate a vector engine | Export and replay procedures still need testing |
| Weaviate | Vector search platform | A team wanting a dedicated retrieval layer and deployment choice | More platform surface enters the runbook |
| Qdrant | Vector search engine | A team wanting explicit control over retrieval infrastructure | Self-managed operation transfers capacity and recovery work to the team |
| Unified REST gateway | Model and backend calls behind HTTP contracts | A small platform team standardizing adapters across languages | Verify that every required capability and region is ready before committing |

Don't store a provider's whole response as the index record. Translate it at the edge into `chunk_id`, vector, model label, dimensions, and token count. Keep the raw transcript and the transformation version so a migration can rebuild the index without calling the old search service. The CRM action writer should consume selected evidence, not a Pinecone, Weaviate, Qdrant, OpenAI, or gateway-specific object.

There is a real suitability limit for this sales-call workflow. A platform whose ASR model is unavailable is not suitable as the transcription source; retain a ready transcription provider and begin this pipeline from completed text. A real-time voice session restricted to the western region is likewise a poor dependency for teams with other residency needs. There is no dedicated moderation endpoint, so moderation requires a chat model with a JSON Schema fallback. Image upscaling is limited to Lanczos. Those last two boundaries do not affect text retrieval directly, but they matter if the same platform decision is supposed to cover adjacent media processing.

## Verify before opening traffic

Run a shadow index first. Feed it a frozen set of sales-call transcripts, including repeated speaker labels, long monologues, corrections, and calls with no follow-up action. Record the expected chunk IDs, total indexing tokens, selected evidence IDs, admitted context tokens, and final structured fields. The acceptance test is not merely semantic similarity. It is repeatability under retry and enough provenance to explain a generated CRM update.

Check four signals during rollout: terminal batch-item counts, duplicate document-version writes, the distribution of admitted context tokens, and retrieval misses on the fixed evaluation set. Alert on a batch that stops making progress, but let the worker retry individual items with the same identity. Treat 429 as backpressure. A 4xx response body should be surfaced with its request identifier and should not enter an unbounded retry loop.

Then pull the plug once.

A useful rollback drill points reads away from the new index version, leaves the old version intact, and replays any accepted-but-not-published CRM actions by their idempotency keys. Do not delete the candidate index during the drill. Quarantine it, compare its manifest with the prior version, and decide whether to resume or rebuild. This is also where a vendor-neutral record earns its keep: rollback changes an index alias or adapter configuration, not every call site.

## Decision rule for the on-call owner

Pick the direct model provider plus a managed vector database when independent scaling and mature, specialized controls matter more than the number of integrations. Pick Weaviate or Qdrant when the team deliberately accepts search-platform operations in exchange for deployment control. Pick a unified REST gateway when language-neutral HTTP, one credential, and consistent discovery reduce integration ownership — after readiness and region checks pass.

Do not optimize for the lowest embedding line item alone. The safe target is a bounded prompt, a traceable retrieval set, and an index that can be replayed somewhere else. For sales-call action extraction, the default should be batch indexing plus token accounting, optional reranking, and chat generation over only the strongest chunks.

## References

- https://platform.openai.com/docs/guides/embeddings
- https://platform.openai.com/docs/guides/batch
- https://docs.pinecone.io/guides/indexes/create-an-index
- https://weaviate.io/developers/weaviate/concepts/vector-index
- https://qdrant.tech/documentation/concepts/collections/
- https://www.rfc-editor.org/rfc/rfc9110.html#name-429-too-many-requests
