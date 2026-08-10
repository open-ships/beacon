# Make connector delivery boundaries explicit

Beacon reports pending delivery relative to each connector route checkpoint and
reports retained history separately. The checkpoint boundary is determined by
delivery class:

| Delivery class | Boundary |
|---|---|
| Confirmed | Successful CAN/file/raw-wire write, HTTP POST 2xx, MQTT QoS 1 broker PUBACK, committed PostgreSQL batch, or intentional null acceptance |
| Resumable | The Envelope is available in the route's retained replay stream |
| Best effort | Plain TCP dispatch completes while downstream receipt remains unknown |
| Observe only | Local inspection completes without a sink write |

MQTT PUBACK confirms broker acceptance, not subscriber receipt. Loss of a
PUBACK after acceptance causes the connector route to retry, so MQTT delivery
is at-least-once and consumers must tolerate duplicates. Beacon disables MQTT
library auto-reconnect and gives each explicit broker connection a fresh client
generation. A loss atomically invalidates that generation and every outstanding
Push. With automatic reconnect disabled, a successful QoS 1 publish token is
possible only after an observed PUBACK; any publish still outstanding at loss
is retried on a newly constructed client. HTTP POST retries use a deterministic
batch idempotency key, but remain at-least-once because the receiver decides
whether to honor it. Receiver `Retry-After` guidance can lengthen, but never
shorten, the connector's retry delay and is capped at one minute. Per-attempt
sink metrics stay separate from confirmed connector counters.

Every connector route also has independent count and logical canonical-JSON
retention guards. Omission means 10,000 messages *and* 64 MiB; it never means
unbounded. Age retention uses Beacon's local `observed_at`/admission time, not
the upstream wire timestamp. Retention may prune pending delivery, so these
guards are safety budgets rather than outage guarantees and must be sized from
observed Envelope rate, size, and the supported disconnected interval.

Remote-source, physical NMEA-endpoint, and confirmed-delivery retries run
continuously with equal-jitter exponential delay from a 250 ms initial ceiling
to one minute. Durable queue writes use 100 ms to one minute. This bounds
wakeups and endpoint pressure through a long vessel outage while preserving
automatic recovery. Network-source, physical-bus, and MQTT connection history
resets only after 30 seconds of stable connectivity, so repeated handshake/drop
flaps do not retry forever at the minimum interval. SSE/WebSocket dial, TLS,
and response-header phases are each bounded to 15 seconds; an established
stream remains long-lived.
Separately, the supervisor continuously converges persisted desired state after
a failed or interrupted hot apply; component-reported remote outages stay with
their own reconnect loops rather than causing restart storms.

These boundaries preserve useful fan-out and automatic recovery without
labeling dispatch as confirmed delivery or implying that bounded retention can
hold an arbitrarily long outage.
