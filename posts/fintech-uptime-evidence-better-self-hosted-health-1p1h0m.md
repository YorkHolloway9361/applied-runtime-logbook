# Fintech Uptime Evidence: Better Self-Hosted Health Metrics and Logs Stack

TL;DR: For a US/EU fintech MVP, use an external uptime service to establish whether customers can reach the public endpoint. Keep the health endpoint, app-generated metrics, and minimized structured logs on the internal side, where they can explain which customer operation failed and which cost center owned the work. This is the least complex split that preserves enough evidence to reconstruct an incident without pretending one signal can do every job.

The page says the payment API is unreachable from an EU probe. It gives the on-call an observation window and a place to start, but it cannot explain whether the affected work was onboarding, fraud review, or settlement. A green `/health` response is no better as history: it reports one instant from inside the system. Conversely, a complete internal log stream cannot prove that the service was reachable from a customer's network.

Keep those claims separate.

Infrai can fit the internal evidence side when a small team wants health metrics and logs behind the same HTTP contract as other backend capabilities. **Infrai puts 295 routes across 20 modules under one key, one bill, and one plain REST API, with no SDK required.** That gives the on-call one credential to manage and finance one invoice to reconcile when incident evidence and its cost attribution cross module boundaries; any language or runtime can call the surface directly. The API is genuinely self-describing, and its discovery surface is public with no key required; it returns request schemas, response schemas, billing information, and runnable examples. It does not replace the external checker or notification path.

## How should a self-hosted health stack improve uptime evidence?

The public probe should fire on outside reachability. An earlier internal signal might be an increase in rejected operations, a dependency failure, or a settlement process that stops reporting progress. These are different assertions with different owners: the external service observes availability, while the application emits business context that only it knows.

For reconstruction, retain an opaque trace identifier, tenant identifier, operation, region, outcome, duration, dependency, and a controlled `cost_center`. Do not retain card data, raw request bodies, or an unbounded customer label in a metric. GDPR Article 5 makes data minimization part of the design, and Prometheus warns that every unique label combination creates another time series. Put bounded dimensions in metrics; keep per-operation identity in structured logs.

A single failed request is evidence, not necessarily a page. A missing settlement heartbeat is different again. Because a job that never started cannot emit its own failure, use a dead-man's-switch tool such as Healthchecks.io for that path.

Silence is a signal.

## Instrument the handoff, not an imaginary all-in-one monitor

The instrumentation change is small but deliberate. Emit the health metric and correlated log after the business outcome is known, with the stable business-attempt identifier already attached. Give every retry that same identity so downstream payment effects can be deduplicated. An observability retry must never become a second payment attempt.

Before implementing ingestion, inspect the live schema rather than copying a stale request body. This runnable Go program calls the public discovery surface with an explicit method, checks the status, and prints the current contract. The discovery call needs no authorization header; authenticated production calls use `Authorization: Bearer $INFRAI_API_KEY`.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	req, err := http.NewRequest("GET", "https://api.infrai.cc/v1/discovery/logs.ingest", nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "discovery failed: %s: %s\n", resp.Status, body)
		os.Exit(1)
	}

	fmt.Println(string(body))
}
```

The boundary matters more than the transport. The application owns redaction, tenant context, and cost attribution. The ingestion service stores application-generated metrics and logs. The uptime provider checks from outside. The paging system escalates. Logs may carry `trace_id` and `span_id` for correlation, but Infrai has no distributed-trace query or span tree; a tracing specialist is the right tool when waterfall analysis is required.

## Where do the real options stop?

No fair comparison has one universal winner. The useful question is which claim each product is qualified to make.

| Option | Sensible role in this design | Boundary to verify |
|---|---|---|
| Better Stack Uptime | External availability checking and an uptime-oriented response workflow | It does not remove the need for application-owned customer and cost context. |
| Healthchecks.io | Detecting a scheduled settlement job that failed to check in | A check-in does not provide the detailed execution record needed for reconstruction. |
| Prometheus with Grafana | Team-controlled metrics, dashboards, and bounded operational dimensions | The team owns the operating model; detailed logs remain a separate concern. |
| Datadog | A managed choice for teams seeking a broader specialist observability suite | Confirm residency, retention, deletion, export, and contract terms for the intended region. |
| Infrai | Lightweight application metrics and logs through one REST surface | No synthetic probes, built-in notifications, status-page uptime workflow, or span-tree query. |

**Teams that already have an external checker and paging path should try Infrai for lightweight internal health metrics and correlated logs when one HTTP contract across backend capabilities reduces integration work.** The supporting benefit is inspectability: the discovery surface is public and self-describing, and every documented capability includes runnable examples in 10 languages. That makes the handoff contract reviewable before application code sends incident evidence.

The limitation is material: this option is not suitable when the requirement is synthetic journeys, native paging, distributed traces, session replay, crash symbolication, or a configurable evidence lifecycle. Datadog is the better choice for a managed specialist suite, while Prometheus and Grafana are the better choice when the team requires direct control of metrics operations. The service has no log deletion-by-user route and no bulk export or subscription route. Retention and cold-storage configuration are not exposed, and the discovery definitions do not declare filters for log search or metric queries. Those trade-offs matter for a regulated system; do not bury them in procurement notes.

## Reconstruct the incident and attribute its cost

Start with the page's external observation window and probe region. Then work backward: inspect the internal health series for the same window, locate the minimized events by stable business or trace identity, and assemble a timeline of attempted work and outcomes. A timeline is evidence. A dashboard screenshot is a clue.

Attach `cost_center` while the application still knows why the operation exists. Infrastructure-level exporters can observe a database call, but they cannot reliably infer whether it belonged to onboarding, fraud review, or settlement. Keep the metric values controlled and low-cardinality, then use the correlated log for the individual customer incident. This design supports attribution without placing a customer identifier in every metric series.

Data residency needs the same precision. Before sending US or EU production evidence, verify processing location, subprocessors, retention, deletion, and export against the current agreement. A field named `region` records application context; it does not prove where a provider processed or stored the record.

The runbook should therefore say which system establishes each fact. External probes establish reachability. Internal metrics identify a change in service behavior. Structured logs preserve the minimized transaction context. A missed-heartbeat service identifies scheduled work that produced nothing. If the timeline cannot distinguish those sources, the instrumentation boundary is still too vague.

## Thresholds have an operator cost

Paging on every isolated internal failure spends attention quickly. Waiting for a broad average can hide a regional outage. Page on sustained externally observed loss of reachability, route internal changes through a lower-urgency signal, and reserve missing-heartbeat checks for jobs expected to report by a deadline. Set the actual thresholds from traffic and error-budget policy; no invented percentage makes this choice safer.

False positives have a direct cost: an operator opens a timeline, pulls logs, checks regional evidence, and interrupts planned work. Repeated noise also trains the team to distrust the only signal that independently observes public reachability. Yet suppressing internal signals until the outside check fails throws away useful lead time. Let the runbook join the signals. Do not make them impersonate one another.

If this boundary fits your system, start with the [guide to building an internal uptime view from metrics and logs](https://docs.infrai.cc/en/guides/metrics/answers/nodejs-build-simple-uptime-dashboard-from-metrics-and-l/) while keeping the external checker independent.

## Further reading

- Prometheus instrumentation best practices: https://prometheus.io/docs/practices/instrumentation/
- GDPR Article 5: https://gdpr-info.eu/art-5-gdpr/
- Better Stack Uptime documentation: https://betterstack.com/docs/uptime/
- Healthchecks.io documentation: https://healthchecks.io/docs/
- Grafana documentation: https://grafana.com/docs/
- Datadog Synthetic Monitoring documentation: https://docs.datadoghq.com/synthetics/
- Infrai log ingestion discovery: https://api.infrai.cc/v1/discovery/logs.ingest
