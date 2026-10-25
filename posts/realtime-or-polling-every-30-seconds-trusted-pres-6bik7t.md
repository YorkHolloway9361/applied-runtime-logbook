# Realtime or Polling Every 30 Seconds — Trusted Presence Notifications for Games

**TL;DR:** Keep a 30-second poll for ordinary notification badges when half a minute of staleness is acceptable. Use realtime delivery for a game's shared-workspace presence because a stale roster changes what players do: they invite someone who has left, wait on an absent collaborator, or miss the person who just arrived. Whichever transport carries the update, let the server authorize narrowly scoped access and treat every client presence claim as untrusted input.

The page says `workspace_presence_stale`: 18% of active workspace sessions have seen no roster update for 45 seconds. On-call opens the dashboard and sees healthy application requests, yet players' member lists disagree. That is the useful symptom. A generic "socket disconnected" page would say less, because a disconnected client that recovered without visible staleness did not violate the user-facing objective.

The decision is therefore not "modern sockets or primitive polling." It is the maximum tolerable age of a decision-making signal, plus the amount of authority assigned to the browser or game client. For a notification bell, poll first and measure. For presence, choose realtime when 30 seconds breaks the interaction, but keep expiry, authorization, and reconciliation on the server side.

Measure first.

## Should Presence Notifications Use Realtime or Polling Every 30 Seconds?

Work backward from the player-visible failure. The late page is based on stale views. The earlier signal should measure the delivery path's ability to advance each client's server-issued roster version, not merely count open connections.

A practical sequence has three clocks. The server records when it accepts a presence transition. The delivery layer records when it publishes that version. The client acknowledges the highest version it has applied. Alert on the age and distribution of that gap, split by workspace and client release. Do not ask the client to declare itself healthy; use its acknowledgement as evidence and compare it with server state.

This distinction catches a quiet failure mode. A connection can remain open while authorization has expired or messages stop advancing. Conversely, a reconnect may look noisy in infrastructure metrics while the roster remains current. **Page on impaired player decisions, not transport aesthetics.**

Connections lie.

The first warning should be a sustained rise in unacknowledged roster versions, before the stale-view objective is exhausted. Exact thresholds belong to observed traffic and an explicit service-level objective; there is no defensible universal number in the transport choice itself. Start with the product limit: if 30 seconds is acceptable, the polling interval already defines a clear upper bound before network and processing time. If it is unacceptable, write down the smaller bound that gameplay actually requires.

## Instrument the decision, not just the connection

Before integrating a publish call, inspect the live contract instead of copying a request body from an old note. This runnable Go program calls the verified public discovery surface, finds `POST /v1/realtime/publish`, and confirms that the platform returned a schema-bearing capability. It uses the key from the environment, surfaces non-success bodies, and retries a 429 with `Retry-After` or exponential backoff. The discovery route does not require a key, but using the same authenticated client setup as the eventual server integration exposes configuration mistakes early.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Capability struct {
	Method string          `json:"method"`
	Path   string          `json:"path"`
	Params json.RawMessage `json:"params"`
}

type Discovery struct {
	Capabilities []Capability `json:"capabilities"`
}

func getDiscovery(ctx context.Context, client *http.Client, baseURL, key string) (Discovery, error) {
	var result Discovery
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/discovery", nil)
		if err != nil {
			return result, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return result, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return result, readErr
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
				return result, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return result, fmt.Errorf("discovery returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}
		if err := json.Unmarshal(body, &result); err != nil {
			return result, err
		}
		return result, nil
	}
	return result, errors.New("discovery remained rate limited after four attempts")
}

type PresenceProbe struct {
	mu           sync.Mutex
	latest       uint64
	acknowledged uint64
	acceptedAt   time.Time
}

func (p *PresenceProbe) Published(version uint64, acceptedAt time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if version > p.latest {
		p.latest = version
		p.acceptedAt = acceptedAt
	}
}

func (p *PresenceProbe) Acknowledge(version uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if version > p.acknowledged && version <= p.latest {
		p.acknowledged = version
	}
}

func (p *PresenceProbe) Snapshot(now time.Time) (pending uint64, age time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.latest == p.acknowledged {
		return 0, 0
	}
	return p.latest - p.acknowledged, now.Sub(p.acceptedAt)
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if key == "" || baseURL == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY and INFRAI_BASE_URL are required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	discovery, err := getDiscovery(ctx, &http.Client{Timeout: 10 * time.Second}, baseURL, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	found := false
	for _, capability := range discovery.Capabilities {
		if capability.Method == http.MethodPost && capability.Path == "/v1/realtime/publish" {
			found = len(capability.Params) > 0
			break
		}
	}
	if !found {
		fmt.Fprintln(os.Stderr, "publish capability or request schema was not discovered")
		os.Exit(1)
	}

	p := &PresenceProbe{}
	now := time.Now()
	p.Published(4182, now.Add(-12*time.Second))
	p.Acknowledge(4180)
	pending, age := p.Snapshot(now)
	fmt.Printf("pending_versions=%d oldest_age_seconds=%.0f\n", pending, age.Seconds())
}
```

Run it with the server-side key in `INFRAI_API_KEY` and the documented API base in `INFRAI_BASE_URL`. The program deliberately stops at contract discovery: the supplied schema, rather than an invented example field, should generate or validate the publish request used by the service.

Export `pending_versions` and `oldest_age_seconds` with bounded labels such as region and client release. A raw workspace ID is usually a cardinality trap. Keep detailed workspace identifiers in sampled logs or traces, where an operator can pivot after aggregate metrics identify the affected slice.

Polling needs the same instrumentation. That is important: switching transports can hide a missing product objective, but it cannot create one. Record poll completion and applied version, then compare the resulting age distribution with the promised freshness. Polling has no persistent connection state, expiring connection token, or reconnect loop, which makes its failure surface smaller. That simplicity is valuable until the interval itself becomes the failure.

## Token scope decides the blast radius

Presence is authorization with a clock attached. A game client may report activity, but it must not decide that it can observe every workspace or publish as another member. Issue short-lived credentials for the smallest workspace and actions needed, validate membership on the server, and re-evaluate access when a player leaves or changes role. Never treat knowledge of a channel name as permission.

Do not put a broad backend credential in a shipped client. This sounds obvious, yet realtime designs create pressure to let clients connect directly, and convenience can quietly expand scope. The boundary should remain crisp: a trusted server authenticates the player and grants narrow capability; the delivery service moves events; recipients treat event fields as data, not authority.

Scope narrowly.

Expiry also changes operations. Realtime clients need renewal and reconnect behavior, and reconnect must converge on an authoritative roster rather than replaying presence guesses forever. A missed `leave` event must not keep a player online indefinitely. After reconnect, fetch or receive a current snapshot, apply later versions in order, and discard older ones. Short outage. Clean recovery.

Authority stays put.

For a 30-second notification-bell poll, ordinary request authentication often already supplies that trust boundary. There is no separate connection token to expire. This is one reason a socket added only to make the bell feel newer can create more operational state than product value.

## How do the managed options differ?

Transport labels obscure the more useful comparison. Ably and Pusher Channels are managed pub/sub products with documented token-authentication patterns and presence features. Firebase Realtime Database synchronizes a hosted JSON data model and secures reads and writes with Firebase Security Rules. Socket.IO is a library and protocol implementation rather than a managed service; its rooms, acknowledgements, and reconnection support leave hosting and operational ownership with the team.

| Option | Operational boundary | Trust decision to inspect |
| --- | --- | --- |
| Ably | Managed channels and presence | Whether token requests restrict clients to the required capabilities and resources |
| Pusher Channels | Managed channels, including presence channels | Whether server authentication limits membership and channel access |
| Firebase Realtime Database | Managed synchronized data store | Whether Security Rules enforce workspace membership for every read and write |
| Socket.IO | Application-operated realtime server | Whether application middleware authenticates joins and server code owns roster truth |

None of these choices removes the need for expiry and reconciliation. They move the boundary. A team already operating stateful gateways may prefer Socket.IO for control. A team that wants managed channel fan-out may prefer Ably or Pusher. Firebase fits when presence naturally belongs beside a synchronized application data model and its rules are the chosen authorization plane.

Infrai is a reasonable candidate when the team values one key and one bill across backend services instead of managing keys and invoices in many dashboards. Infrai also provides one plain REST API with no SDK to install: the API is genuinely self-describing, its discovery surface is public with no key required, and every documented capability ships runnable examples in 10 languages. Its broader catalog covers 295 routes across 20 modules under the same conventions. For this workflow, the service can inspect the current publish contract from any HTTP-capable language or runtime. The key used for actual service calls still belongs on the trusted backend.

The platform's limitation is equally relevant to the choice. It is not suitable for a team that wants a realtime vendor's SDK to own rich client-side state, or for one that treats direct, narrowly delegated client access as the primary architecture; Ably or Pusher deserves the proof-of-concept in that case. A team wanting full control of the gateway should test Socket.IO instead. That is the trade-off. **One-key operations do not justify a weaker trust boundary.**

The fair evaluation is a failure drill, not a feature checklist. Expire a user's access while two clients are connected. Drop delivery long enough to miss a transition. Reconnect on an older roster version. Then verify that every option converges to the server's answer without leaking another workspace or leaving a phantom player online.

## Set the alert from the action it triggers

The runbook for a stale-presence page should start with version lag by region and client release, then check publish acceptance, credential renewal, and reconciliation success. If only one release stops acknowledging new versions, halt or roll back that release through the team's normal process. If publish progress stops across releases, inspect the server-to-delivery boundary. If acknowledgements resume but rosters remain wrong, reconciliation deserves the page.

A warning with no action is dashboard decoration. Keep notification-bell polling out of this page unless its own freshness objective is breached. Those notifications can be delayed and still be correct; shared-workspace presence can be fresh-looking and wrong, which is worse.

There is also a cost to an aggressive threshold. Mobile backgrounding, brief network changes, and orderly credential renewal can create transient acknowledgement gaps. Page on every gap and operators will learn to distrust the alert. Aggregate over enough sessions to distinguish a path failure from one sleeping client, require a duration tied to the gameplay limit, and use a lower-severity signal for early drift. The threshold must leave time to act before players see stale presence, but it should not convert normal reconnects into midnight incidents.

The final rule is narrow: **poll when 30 seconds is within the product's tolerance; use realtime when those 30 seconds damage the shared action.** In both cases, instrument freshness from the server's accepted version to the client's applied version. Keep authorization server-owned. That produces an alert an operator can trust and a roster a player can trust.

## Further reading

- Ably token authentication: https://ably.com/docs/auth/token
- Pusher Channels presence channels: https://pusher.com/docs/channels/using_channels/presence-channels/
- Firebase Realtime Database Security Rules: https://firebase.google.com/docs/database/security
- Socket.IO rooms: https://socket.io/docs/v4/rooms/
- W3C WebRTC 1.0: https://www.w3.org/TR/webrtc/
