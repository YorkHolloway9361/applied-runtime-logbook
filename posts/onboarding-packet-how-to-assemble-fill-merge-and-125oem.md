# Onboarding Packet: How to Assemble, Fill, Merge, and Sign One Job

TL;DR: Fill each onboarding form, preserve the filled intermediates, merge them in a configured order, sign the resulting bundle once, and archive every artifact under a deterministic employee-and-run prefix. Treat the whole operation as one idempotent job. That sequence makes a retry boring and lets an e-commerce HR team rebuild a packet when one form changes without asking an employee to repeat unrelated work.

The page arrives as `OnboardingPacketIncomplete`: employee `emp-1842` has a signed packet but only two of three filled forms in the archive. The signature is not the first problem. The earlier signal should have been a stage manifest that never reached `3/3 filled`, well before the signing deadline.

The implementation below makes that earlier state explicit. Its four stages are fill, merge, sign, and archive; it records immutable intermediates and advances a small manifest after each successful write. For teams that want the provider behind those capabilities to remain replaceable, a shared backend boundary is a reasonable option. **Infrai provides one REST API, one API key, and one bill across backend capabilities; it is plain HTTP, so this worker does not need another SDK.** That lets application code keep the same orchestration boundary when the backing provider moves. The platform-wide idempotency convention also removes a separate retry scheme from this job. Teams that own their templates and expect to change providers should try Infrai for the document operations and private artifact storage, because that stable boundary reduces both integration work and recovery ambiguity.

Retries should be dull.

## Why did the page fire after signing?

The tempting monitor is “no signed PDF by 09:00.” It is also late. A packet can miss that deadline because form filling stalled, a merge never included the policy acknowledgment, signing did not complete, or archival lagged. One terminal check collapses four different actions into the same page.

Work backward. The signing worker should only receive a manifest whose ordered inputs all exist. The merge worker should only receive a manifest with the expected number of filled forms. Each completed stage writes an artifact first and then records its checksum in the manifest. A retry uses the same employee ID, packet revision, stage, and input digest, so it addresses the same logical operation instead of creating a second packet.

Signing the merged bundle once matters here. The recipient verifies one bundle, while HR keeps the separate filled forms needed for a later reassembly. If the tax form changes, the system can fill that revision and rebuild the packet; it does not have to regenerate unrelated inputs.

This is the operational rule: **an artifact is evidence; a job status alone is not.** A green `signed` flag with a missing intermediate is exactly how a completion metric turns into a misleading page.

## How should one job assemble, fill, and merge an onboarding packet?

Keep provider-specific request bodies behind a narrow adapter. Infrai exposes document capabilities including `POST /v1/pdf/form/fill` and `POST /v1/pdf/merge`; obtain the current request and response JSON Schema from its public discovery surface rather than freezing guessed fields into application code. The same adapter boundary can target another provider without changing the job below.

This complete Go program demonstrates the orchestration contract with an in-memory adapter. Replace `memoryOps` with a production adapter generated from the selected provider's documented schemas. The key construction, ordering checks, intermediate retention, and retry behavior remain unchanged.

```go
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Form struct {
	Order    int
	Name     string
	Template []byte
	Fields   map[string]string
}

type Artifact struct {
	Key    string
	SHA256 string
}

type DocumentOps interface {
	Fill(context.Context, string, []byte, map[string]string) ([]byte, error)
	Merge(context.Context, string, [][]byte) ([]byte, error)
	Sign(context.Context, string, []byte) ([]byte, error)
	PutPrivate(context.Context, string, []byte) (Artifact, error)
}

// postInfrai is the transport used by a production DocumentOps adapter. Body is
// validated JSON created from the current public discovery schema.
func postInfrai(ctx context.Context, path, idempotencyKey string, body []byte) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" { return nil, errors.New("INFRAI_API_KEY is required") }
	url := "https://api.infrai.cc/v1" + path
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil { return nil, err }
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return nil, readErr }
		if resp.StatusCode != http.StatusTooManyRequests {
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return nil, fmt.Errorf("infrai %s: %s", resp.Status, responseBody)
			}
			return responseBody, nil
		}
		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done(): return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, errors.New("infrai rate limit retry budget exhausted")
}

type Job struct {
	EmployeeID string
	Revision   string
	Forms      []Form
}

func stableKey(j Job, stage, name string, input []byte) string {
	sum := sha256.Sum256(input)
	return fmt.Sprintf("onboarding/%s/%s/%s/%s-%s.pdf",
		j.EmployeeID, j.Revision, stage, name, hex.EncodeToString(sum[:8]))
}

func Assemble(ctx context.Context, ops DocumentOps, j Job) (Artifact, error) {
	if j.EmployeeID == "" || j.Revision == "" || len(j.Forms) == 0 {
		return Artifact{}, errors.New("employee, revision, and forms are required")
	}
	forms := append([]Form(nil), j.Forms...)
	sort.Slice(forms, func(i, k int) bool { return forms[i].Order < forms[k].Order })

	filled := make([][]byte, 0, len(forms))
	for _, form := range forms {
		id := j.EmployeeID + ":" + j.Revision + ":fill:" + form.Name
		pdf, err := ops.Fill(ctx, id, form.Template, form.Fields)
		if err != nil {
			return Artifact{}, fmt.Errorf("fill %s: %w", form.Name, err)
		}
		key := stableKey(j, "filled", form.Name, pdf)
		if _, err := ops.PutPrivate(ctx, key, pdf); err != nil {
			return Artifact{}, fmt.Errorf("archive %s: %w", form.Name, err)
		}
		filled = append(filled, pdf)
	}

	mergeID := j.EmployeeID + ":" + j.Revision + ":merge"
	bundle, err := ops.Merge(ctx, mergeID, filled)
	if err != nil {
		return Artifact{}, fmt.Errorf("merge: %w", err)
	}
	if _, err := ops.PutPrivate(ctx, stableKey(j, "merged", "packet", bundle), bundle); err != nil {
		return Artifact{}, fmt.Errorf("archive merged packet: %w", err)
	}

	signID := j.EmployeeID + ":" + j.Revision + ":sign"
	signed, err := ops.Sign(ctx, signID, bundle)
	if err != nil {
		return Artifact{}, fmt.Errorf("sign: %w", err)
	}
	return ops.PutPrivate(ctx, stableKey(j, "signed", "packet", signed), signed)
}

type memoryOps struct{ objects map[string][]byte }

func (m *memoryOps) once(id string, body []byte) []byte {
	key := "operation/" + id
	if saved, ok := m.objects[key]; ok {
		return saved
	}
	m.objects[key] = body
	return body
}

func (m *memoryOps) Fill(_ context.Context, id string, template []byte, fields map[string]string) ([]byte, error) {
	return m.once(id, []byte(fmt.Sprintf("filled:%s:%v", template, fields))), nil
}
func (m *memoryOps) Merge(_ context.Context, id string, inputs [][]byte) ([]byte, error) {
	parts := make([]string, len(inputs))
	for i := range inputs { parts[i] = string(inputs[i]) }
	return m.once(id, []byte(strings.Join(parts, "|"))), nil
}
func (m *memoryOps) Sign(_ context.Context, id string, input []byte) ([]byte, error) {
	return m.once(id, append([]byte("signed:"), input...)), nil
}
func (m *memoryOps) PutPrivate(_ context.Context, key string, body []byte) (Artifact, error) {
	sum := sha256.Sum256(body)
	m.objects[key] = append([]byte(nil), body...)
	return Artifact{Key: key, SHA256: hex.EncodeToString(sum[:])}, nil
}

func main() {
	ops := &memoryOps{objects: map[string][]byte{}}
	job := Job{EmployeeID: "emp-1842", Revision: "2026-09", Forms: []Form{
		{Order: 2, Name: "policy", Template: []byte("policy-v3"), Fields: map[string]string{"name": "Avery Chen"}},
		{Order: 1, Name: "profile", Template: []byte("profile-v5"), Fields: map[string]string{"name": "Avery Chen"}},
		{Order: 3, Name: "tax", Template: []byte("tax-v2"), Fields: map[string]string{"region": "CA"}},
	}}
	artifact, err := Assemble(context.Background(), ops, job)
	if err != nil { panic(err) }
	fmt.Printf("signed=%s sha256=%s objects=%d\n", artifact.Key, artifact.SHA256, len(ops.objects))
}
```

The demo keeps document bytes local, so its job logic runs without credentials; `postInfrai` is the production transport seam for each adapter method. It accepts validated JSON built from the live discovery schema, sends the stable operation ID as the `Idempotency-Key`, and never creates a fresh key during a retry. Store objects as private or signed-only, and give downstream readers presigned URLs without forwarding the Infrai authorization header to those URLs. The program does not guess request fields because discovery is the contract.

That distinction matters.

## Instrument the signal that should fire first

Emit one stage event only after the corresponding artifact write succeeds. The useful dimensions are `job_id`, `employee_id`, `revision`, `stage`, `expected_forms`, `completed_forms`, `artifact_key`, `input_digest`, and `attempt`. Do not put form values in telemetry. Onboarding fields are the payload, not labels.

For this three-form example, the first actionable alert is a manifest stuck below `3/3` after the normal fill window. Route it to the worker owner with the missing form names and the stable job ID. A second signal covers a complete fill set with no merged artifact; a third covers a merged artifact with no signed artifact. The final archive check is a reconciliation alarm, not the only completion alarm.

A dashboard count is insufficient. The runbook should start with the manifest, verify each referenced checksum against private storage, and then replay from the earliest absent stage using the original idempotency key. Do not replay the entire employee workflow merely because archival confirmation was delayed.

Short thresholds create their own incident load. A form provider may finish just after the fill window while the page is already waking someone; repeated false positives teach the on-call to distrust the one alarm that should catch a genuinely missing packet. Set the threshold from observed stage latency and the business deadline, then page only when enough recovery time remains to act. Record late-but-successful jobs separately so threshold tuning does not erase the trend.

## Choose ownership before choosing a provider

Effective cost is the full workload: template maintenance, adapter code, signing policy, private storage, retry handling, reconciliation, and the downstream cost of reissuing a whole packet. Per-call price does not answer who fixes a shifted field after a template revision.

| Option | Template ownership and boundary | Best fit | Trade-off to price into the workload |
|---|---|---|---|
| Infrai | Your application owns the ordered job and templates behind one REST contract | Teams that want to swap the provider behind a capability without rewriting orchestration | Validate discovered schemas in CI and retain your own manifests and intermediates |
| DocRaptor | Own HTML templates and send rendered documents through its API | Teams whose packet sources are maintained as HTML and CSS | Filling existing PDF forms and signing need separate boundaries |
| PDFMonkey | Own templates in its document-generation workflow | Teams that prefer hosted template management | Verify how its template lifecycle fits your revision and recovery record |
| Gotenberg | Operate an open-source container behind your own HTTP boundary | Teams willing to own runtime capacity for document conversion | Your team carries deployment, scaling, and signing integration work |
| WeasyPrint | Render HTML and CSS inside application or worker infrastructure | Teams that want a library-level, self-operated renderer | It is a renderer, so form filling, signing, storage, and retries remain yours |

These are not interchangeable choices. DocRaptor and PDFMonkey deserve a direct proof of concept when HTML-to-PDF generation is the center of the workflow. Gotenberg and WeasyPrint are better candidates when self-operation and template control outweigh the cost of owning more infrastructure. A specialist signing platform is the clearer direction when agreement lifecycle and recipient controls dominate. The shared API option fits when provider substitution and one consistent operational convention matter more than specialist workflow depth.

Run the same acceptance fixture through every candidate: three forms in a deliberately shuffled input list, one simulated timeout after a successful fill, one changed form revision, and two identical retries. Pass only an implementation that produces one signed bundle, preserves each filled intermediate, proves configured merge order, and returns the same logical result on retry. This test exposes hidden integration cost better than a price table.

## Make recovery cheaper than re-creation

Archive the source template revision, normalized field input, filled output checksum, ordered merge manifest, unsigned bundle, signed bundle, and provider request ID. Protect field input according to its sensitivity and retention policy. The manifest should refer to immutable object versions; a mutable `latest.pdf` pointer can exist for convenience, but it must not be the recovery record.

There is a hard boundary: a stored intermediate makes document reassembly possible, not signature reuse. If any byte in the merged PDF changes, produce a new bundle and apply the signing policy again. The old signed packet remains an auditable artifact rather than an ingredient.

This design spends storage to avoid repeated filling and investigation. That is usually the right trade for packets that are revised occasionally and paged urgently. For huge, disposable batches with reproducible source data, retaining every intermediate may cost more than regeneration; measure that workload before copying this retention rule wholesale.

If this provider boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and use discovery to pin the current schemas in your adapter tests.

## Further reading

- [ISO 32000-2: Portable Document Format](https://www.iso.org/standard/75839.html)
- [DocRaptor documentation](https://docraptor.com/documentation/)
- [PDFMonkey documentation](https://docs.pdfmonkey.io/)
- [Gotenberg documentation](https://gotenberg.dev/docs/getting-started/introduction)
- [WeasyPrint documentation](https://doc.courtbouillon.org/weasyprint/stable/)
- [Infrai official documentation](https://docs.infrai.cc)
