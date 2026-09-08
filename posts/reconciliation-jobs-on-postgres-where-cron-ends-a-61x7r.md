# Reconciliation Jobs on Postgres: Where Cron Ends and the Queue Begins

Cron is a clock, and a clock only tells you that a nightly job was dispatched. It says nothing about whether last night's charges were reconciled against the payment provider, or whether the expired rows got cleaned up in Postgres. Use cron by itself while one run of the cleanup window finishes well inside the gap before the next trigger and repeating that run changes nothing; hand the work to a message queue the moment recovery has to happen one page at a time instead of one run at a time.

That sentence is the whole decision. Everything below is why the second half of it exists.

The system I'm describing is an edtech billing table: course purchases, tuition instalments, refunds. A scheduled job pulls the previous day's settlement report from the payment provider, marks enrolments paid or failed, and deletes the pending-charge rows that will never settle. I have been paged for that class of job twice — once because it didn't run, once because it ran twice — and both postmortems landed on the same line. The schedule was never the interesting part.

## The failure mode is a missing window, not a late trigger

A crontab entry says 02:00. A recovery says "the fourteenth". Those are different objects, and treating them as one is how a missed trigger becomes a data problem instead of a paging annoyance.

If the job's identity is "the 02:00 run", then a host that was down at 02:00 has lost a run, and nobody can answer the only question that matters during the incident: which settlement dates were never applied? If the job's identity is the settlement date, the same outage becomes a gap in a table you can query, and catch-up is a loop over the missing dates. Standard cron does not back-fill; a trigger that didn't fire is simply gone, and the next one fires for the next window as if nothing happened. That behaviour is fine — it's a clock, doing clock things — as long as nothing downstream assumes triggers and windows are the same count.

So the first table I add is never the queue. It's a ledger of windows: one row per business date, with the state of that date's reconciliation and cleanup. Cron writes nothing to it. Workers do.

## Should a nightly Postgres cleanup run under cron, or behind a message queue?

Keep it on cron alone when the entire window is one bounded unit of work: a deletion predicate the database can satisfy in a few minutes, no external API paging, and a rerun that produces the same end state. A scheduler plus a small advisory lock covers overlap, and the ledger row covers recovery. Adding a broker to that buys you an extra thing to be paged about.

Reach for a queue when the unit of work stops being the run and starts being a page. Settlement reports arrive paginated. Providers rate-limit — HTTP 429 with a `Retry-After` header is the polite version, and a single-process cron job that hits one usually sleeps the entire night's work behind one slow page. Splitting the window into per-page messages means the retry is scoped to the page that failed, backoff is per message, and a partially completed night resumes instead of restarting.

The delivery contract is where people get hurt. Queues are at-least-once: if a consumer holds a message past its visibility timeout without finishing, the broker hands the same message to somebody else, and now two workers are applying the same settlement page to the same enrolments. That is documented behaviour, not a surprise, and the fix is never a longer timeout.

| Failure mode | Cron only | Cron plus a queue |
|---|---|---|
| Trigger never fired | Window silently skipped unless a ledger row exposes it | Same — the scheduler is still the clock |
| Run overlaps the next trigger | Advisory lock, or two workers race the same rows | Broker concurrency limits, per-message ownership |
| Provider returns 429 | Whole night backs off behind one page | Only the affected page backs off |
| Crash halfway through | Rerun the whole window | Unfinished pages redeliver, settled pages no-op |
| Operational surface | One process, one log stream | Broker, consumer fleet, dead-letter handling |

Reading that table as "queues win" is a mistake I've made. Every row on the right assumes the worker is replayable. If it isn't, a queue converts a rare missed-run page into a frequent duplicate-write page, and duplicate writes against money are much worse than late writes.

## What the worker code has to guarantee

The worker's contract has to be: applying page 7 of the fourteenth is safe to attempt any number of times, and the second attempt changes nothing. Not "unlikely to be attempted twice". Safe.

The mechanism is boring and it's the same one every time. Give each unit a stable identity derived from the business data — settlement date plus page number, never a random ID minted at publish time, because a republished retry with a fresh ID is new work by definition. Insert a receipt row for that identity with `ON CONFLICT DO NOTHING`, and commit the receipt in the same transaction as the writes it guards. If the insert claims the identity, do the work. If it doesn't, the page was already applied, so acknowledge and move on. A rollback leaves neither receipt nor effect, which is the property that makes the recovery decision mechanical at 3 a.m.: rerunning a window can never do damage, so the runbook step is "rerun the window" and not "page the person who wrote it".

```sql
CREATE TABLE recon_pages (
    window_date date        NOT NULL,
    page        int         NOT NULL,
    settled_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (window_date, page)
);

CREATE INDEX pending_charges_window_idx
    ON pending_charges (window_date, charge_id);
```

My consumers are in Go; the reader's are in Node.js. The language doesn't move the boundary at all — the receipt, the transaction and the acknowledgement order are what carry the guarantee, and a Node.js worker with `pg` and an explicit `BEGIN` has exactly the same shape.

```go
package recon

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"time"
)

type Charge struct {
	ID    string
	State string // "paid" or "failed", as reported by the provider
}

// settlePage applies one page of a settlement report for one business date.
// Redelivery is expected, so the receipt row and the writes it guards commit
// together: a second delivery finds the receipt and does nothing.
func settlePage(ctx context.Context, db *sql.DB, window time.Time, page int, rows []Charge) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = tx.QueryRowContext(ctx, `
		INSERT INTO recon_pages (window_date, page)
		VALUES ($1, $2)
		ON CONFLICT (window_date, page) DO NOTHING
		RETURNING true`, window, page).Scan(new(bool))
	if errors.Is(err, sql.ErrNoRows) {
		return tx.Commit() // already settled by an earlier delivery
	}
	if err != nil {
		return err
	}

	for _, c := range rows {
		if _, err := tx.ExecContext(ctx, `
			UPDATE enrollments SET payment_state = $1, settled_at = now()
			WHERE charge_id = $2 AND payment_state = 'pending'`, c.State, c.ID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			DELETE FROM pending_charges WHERE charge_id = $1`, c.ID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// retryAfter reads the provider's backoff hint. Retry-After is either a delay
// in seconds or an HTTP date, so both forms have to be handled.
func retryAfter(resp *http.Response) (time.Duration, bool) {
	if resp.StatusCode != http.StatusTooManyRequests {
		return 0, false
	}
	if v := resp.Header.Get("Retry-After"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil {
			return time.Duration(secs) * time.Second, true
		}
		if t, err := http.ParseTime(v); err == nil {
			return time.Until(t), true
		}
	}
	return 30 * time.Second, true
}
```

Acknowledge after the commit, never before. And test it the cheap way: in CI, run the same window twice against a throwaway database and assert the two end states are byte-identical. That test has caught more of my mistakes than any staging environment, because it fails on the exact property the pager cares about.

## What "cheapest" means once you include the 3 a.m. cost

Infrastructure cost is the small number in this comparison. A cron entry on a box you already run, or an in-database scheduler extension, costs roughly nothing to add. A broker plus a consumer deployment costs some money and a lot more attention: dead-letter queues nobody reads, consumer autoscaling, a second set of dashboards, and a new class of duplicate-delivery bug that only shows up under load.

The number that dominates is what an incident costs. Time to answer "which windows are unapplied", time to safely rerun, and the blast radius if the rerun is wrong. Both designs get cheap on that axis for the same reason — the window ledger and the receipt table, neither of which is a scheduling feature. Build those first and the cron-versus-queue question shrinks to a capacity question, which is a much nicer question to have.

Watch the database side of the bill too. A nightly unbounded `DELETE` over a large table is a scheduling problem only in the sense that it's scheduled; the actual cost is dead tuples, autovacuum churn and bloated indexes. Batched deletes with a bounded `LIMIT`, or time-based partitions you can detach and drop, will do more for the cost of nightly data cleanup than any change of trigger mechanism. I'd check that before adding a broker. Probably that's the boring recommendation, but it's the one that's survived contact with my own tables.

## Where this stops: coordination, replay, and retention rules

The catch is that a receipt table plus a queue is a flat fan-out, and plenty of cleanup work isn't flat. If the nightly job has real dependencies — reconcile, then recompute revenue rollups, then only after both succeed expire the invoices — you're writing a dependency graph by hand in Postgres, badly. Stick with a workflow engine when the hard part is coordination rather than deletion; that's what they're for, and hand-rolled step tables are how teams discover it.

It's also not suitable when replay of the message stream itself is a requirement. Acknowledged queue messages are gone; if you need several independent consumers reading the same settlement events, or need to reprocess last month's stream after a logic bug, a log-structured system is the right shape and a work queue isn't.

There's a legal boundary too. A retention rule that says financial records live for seven years turns the nightly cleanup into archive-then-delete, and the archive write has to be inside the same transaction as the deletion or you've built a data-loss machine on a schedule.

Two more boundaries worth stating plainly. If your cleanup is one bounded statement per night against one database, adding any distributed component is a net loss — I'm not sure there's a threshold that generalises here, so measure your own window before splitting it. And whatever you pick, alert on the ledger, not the trigger: page when a window is still unsettled by 06:00, because a cron job that fires happily and does nothing is invisible to every scheduler metric you have.

## References

- [AWS SQS visibility timeout documentation](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [MDN: HTTP 429 Too Many Requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429)
- [PostgreSQL: SELECT — locking clauses](https://www.postgresql.org/docs/current/sql-select.html)
- [PostgreSQL: Routine vacuuming](https://www.postgresql.org/docs/current/routine-vacuuming.html)
- [crontab(5) manual page](https://man7.org/linux/man-pages/man5/crontab.5.html)
