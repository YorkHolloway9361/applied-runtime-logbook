# Compatible Summarization API with One Key — Chat Completions Cost by Tenant

Route every sales-call summary through a tenant-aware job envelope, record usage beside the resulting CRM actions, and switch models only after replaying the same contract tests. The deciding constraint is attribution: a single key and a compatible chat-completions shape may simplify integration, but they don't tell an operator which tenant caused spend, which prompt produced an action, or whether a retry duplicated a write.

Short answer: choose the API path that preserves tenant identity, structured action fidelity, idempotent delivery, and auditable usage across model changes; compare cost only after those controls pass.

For a healthtech sales workflow, the unit of work is not "some text." It is a call transcript that may contain contact details and health-related context, followed by CRM mutations with owners and due dates. The runbook therefore starts at the queue boundary, not at a provider logo. Keep OpenAI, Claude, and Gemini behind the same internal contract if that helps the application, but treat compatibility as a hypothesis that must be tested.

No blind switches.

## What signal should page the summarization API owner?

Page on lost or unsafe business outcomes, not on a provider-shaped success counter. A successful chat response can still produce an empty action list, an invalid due date, or a summary that belongs to the wrong tenant. The primary service-level signals should cover jobs that exceed their completion deadline, terminal jobs without a durable result, duplicate CRM mutation attempts, schema rejection rate, and usage records that cannot be joined to a tenant.

Use a lifecycle with explicit states such as `queued`, `leased`, `summarized`, `validated`, `applied`, and `dead_lettered`. The worker may retry model calls, but the CRM writer must accept an idempotency key derived from immutable inputs: tenant ID, call ID, transcript revision, prompt version, and requested operation. A retry then repeats work without creating a second follow-up task. Keep the generated summary and proposed actions separate from the applied CRM receipt; that boundary makes reconciliation possible.

A concrete failure is easier to reason about. Suppose job `sum_tenant-042_call-918_rev-3` finishes twice because its lease expires while the first worker is still validating output. Both attempts may be valid, and neither needs to be treated as an outage. Only one is allowed to move action `follow_up_2026-08-18` into the CRM. The second attempt should observe the existing idempotency receipt, mark itself reconciled, and emit no duplicate mutation. This is why queue delivery count is diagnostic context rather than a business-success metric. The page fires if the receipt is missing after the deadline, not merely because delivery count reached `2`.

The cost signal follows the same rule. Store input units, output units, model identifier, tenant ID, job ID, and the provider's usage record when one is returned. Don't estimate a tenant's bill from transcript characters after the fact and call it precise. If comparable usage data is incomplete, label the estimate and retain the raw fields needed to recompute it. I'm not sure any cross-provider unit normalization will stay fair without a versioned definition; a finance-approved conversion rule and periodic reconciliation would resolve that uncertainty.

## How should a compatible summarization API compare model cost during switching?

Start with a shadow replay set sampled by tenant and call shape. It should include short calls, long calls, no-action calls, several-action calls, corrected transcripts, and transcripts containing language that must not become a CRM instruction. Send the same versioned semantic request through each adapter, but don't assume that identical chat-completions fields guarantee identical semantics. OpenAI, Claude, and Gemini are three real candidates for this test matrix; the comparison is the observed result under your contract, not a ranking copied from a feature page.

The gate needs both quality and accounting columns:

| Gate | Per-job evidence | Reject the candidate when |
| --- | --- | --- |
| Action fidelity | Expected and actual action tuples | Required owner, due date, or disposition changes |
| Schema safety | Validator result and rejected field path | Invalid output can reach the CRM writer |
| Tenant attribution | Tenant ledger row joined to job and receipt | Any completed job is unallocated |
| Retry safety | Stable idempotency key and mutation receipt | Replay creates another CRM action |
| Cost comparison | Versioned usage fields and normalized ledger entry | The calculation cannot be reproduced |
| Data handling | Approved flow, retention rule, and access record | The path violates the organization's controls |

Compare distributions per tenant rather than one global average. A tenant with ninety-second calls and one action should not subsidize, or be hidden by, a tenant with forty-minute calls and eight actions. Track cost per accepted summary, cost per applied action set, rejection rate, and operator-review rate. A model with a lower apparent unit charge can be the worse operational choice if more outputs need review; the ledger should expose that relationship without turning price into the sole decision.

The catch is measurement overhead. Per-tenant ledgers add cardinality, retention decisions, and reconciliation work. They are not suitable when the workload cannot legally or operationally retain the necessary attribution fields. In that case, aggregate at the narrowest approved boundary and accept that tenant-level showback is unavailable. Also stick with a provider-specific integration when a required semantic feature cannot be represented honestly by the common contract. A leaky abstraction is worse than two explicit adapters.

## Put the safe boundary in code

The application should own a small semantic interface and a durable job record. It should not expose a provider's response object to the CRM writer. This Go sketch keeps model invocation, validation, accounting, and mutation in an order that supports replay:

```go
package summarize

import (
    "context"
    "errors"
    "fmt"
)

type Job struct {
    ID             string
    TenantID       string
    CallID         string
    TranscriptRev  int
    PromptVersion  string
    Transcript     string
    Model          string
}

type Action struct {
    Kind    string `json:"kind"`
    Owner   string `json:"owner"`
    DueDate string `json:"due_date"`
}

type Result struct {
    Summary string   `json:"summary"`
    Actions []Action `json:"actions"`
}

type Usage struct {
    InputUnits  int64
    OutputUnits int64
    RawRef      string
}

type ModelClient interface {
    Summarize(ctx context.Context, model, transcript string) (Result, Usage, error)
}

type Ledger interface {
    Record(ctx context.Context, tenantID, jobID, model string, usage Usage) error
}

type CRM interface {
    ApplyOnce(ctx context.Context, idempotencyKey, tenantID, callID string, actions []Action) error
}

type Validator interface {
    Check(Result) error
}

type Worker struct {
    Models    ModelClient
    Ledger    Ledger
    CRM       CRM
    Validator Validator
}

func (w Worker) Run(ctx context.Context, job Job) error {
    result, usage, err := w.Models.Summarize(ctx, job.Model, job.Transcript)
    if err != nil {
        return fmt.Errorf("summarize %s: %w", job.ID, err)
    }

    if err := w.Validator.Check(result); err != nil {
        return fmt.Errorf("validate %s: %w", job.ID, err)
    }
    if len(result.Actions) == 0 && result.Summary == "" {
        return errors.New("empty result")
    }

    if err := w.Ledger.Record(ctx, job.TenantID, job.ID, job.Model, usage); err != nil {
        return fmt.Errorf("record usage %s: %w", job.ID, err)
    }

    key := fmt.Sprintf("%s:%s:%d:%s",
        job.TenantID, job.CallID, job.TranscriptRev, job.PromptVersion)
    if err := w.CRM.ApplyOnce(ctx, key, job.TenantID, job.CallID, result.Actions); err != nil {
        return fmt.Errorf("apply actions %s: %w", job.ID, err)
    }
    return nil
}
```

The interfaces are intentionally boring. The adapter may translate a common request into a vendor-specific call, while the worker sees one result contract. `Record` must itself be idempotent on the job and accounting version, and `ApplyOnce` must enforce uniqueness in durable storage; an in-memory check is not enough. For sensitive data, decide before implementation which fields may enter prompts, logs, traces, replay fixtures, and dead-letter records. HIPAA's administrative, physical, and technical safeguard requirements are a compliance boundary, not a checkbox supplied by API compatibility. Have the responsible privacy and security teams approve the actual data flow.

One ordering detail deserves scrutiny — usage is recorded before CRM mutation. That preserves cost evidence when the downstream write is retried, but it means the ledger needs separate `generated` and `applied` outcome fields. If your accounting store is temporarily unavailable, don't silently apply actions and lose attribution. Release the lease for a bounded retry, using the same job ID and idempotency keys.

## Verify deployment before moving tenant traffic

Deploy adapters and ledger changes dark first. Replay a fixed, approved corpus and compare candidate output against invariants, not prose similarity alone. A useful preflight checks that every job has one terminal state, every accepted output passes the schema, every CRM receipt maps to one idempotency key, every recorded usage row maps to a tenant, and sensitive transcript content is absent from logs.

Then canary by tenant, because tenant identity is the dimension the system must preserve. Start with an internal or explicitly approved tenant cohort, pin its adapter and model in configuration, and keep the previous choice available. Watch accepted summaries, action review, end-to-end age, retry counts, ledger reconciliation, and CRM receipts together. A green model-call graph with a growing queue age is still a failed canary. So is a clean queue with unattributed usage.

Model switching should be a configuration change carrying a change ID, model identifier, prompt version, adapter version, cohort, start time, and rollback target. Never encode the choice only in a process-wide environment variable when different tenants can be on different cohorts. The audit question is specific: which model and prompt produced the actions applied to call `call-918`, and which usage record funded them? The answer should require one join, not a log search.

Keep alerts sparse enough to act on. Page for deadline risk, duplicate mutation, or unreconciled accounting beyond the agreed window. Ticket slow drift in review rate or cost per accepted result. Dashboard raw provider latency and unit counts for diagnosis, but don't confuse them with the user-visible objective.

## Roll back the model, not the evidence

Rollback means stopping new leases for the canary configuration, restoring the last approved tenant-to-model mapping, and letting already leased jobs finish or expire according to a documented rule. Do not delete results, ledger rows, or mutation receipts. Mark superseded summaries and retain their lineage under the applicable retention policy.

After rollback, replay only jobs whose CRM receipt proves that no action set was applied, unless the business owner explicitly requests a corrected action. Jobs with an existing receipt go through reconciliation. This distinction prevents a model rollback from becoming a duplicate-delivery incident.

The final selection rule is plain: adopt a shared summarization path only when it keeps tenant attribution complete, CRM actions idempotent, output quality inside the agreed contract, and accounting reproducible. If one candidate fails those gates, keep the current model or its explicit adapter while the gap is evaluated. One key is convenient. Operability comes from the envelope around it.

## References

- https://python.langchain.com/docs/integrations/chat/openai/
- https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-C/part-164
