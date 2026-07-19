# Beacon NMEA 2000 Integration

Beacon observes NMEA 2000 networks and moves messages between marine data endpoints while preserving enough wire truth for diagnosis, replay, and controlled forwarding.

## Language

**Bus endpoint**:
A physical or remote connection to one NMEA 2000 network segment.
_Avoid_: Interface, port, adapter when referring to the network connection as a whole

**Device NAME**:
The stable 64-bit ISO 11783 identity claimed by an NMEA 2000 device independently of its current source address.
_Avoid_: Device ID, address

**Appliance identity**:
Beacon's persistent Device NAME and product/configuration information presented on every independently connected bus endpoint.
_Avoid_: Random client identity

**Envelope**:
Beacon's canonical representation of one assembled NMEA 2000 message, containing raw wire data plus additive semantic metadata.
_Avoid_: Packet, event

**Connector route**:
One configured path from a source through filters and a durable queue to a sink.
_Avoid_: Pipeline, link

**Pending delivery**:
Messages after a connector route's delivery checkpoint that have not yet reached that route's declared delivery boundary.
_Avoid_: Queue depth when retained history is included

**Retained history**:
Acknowledged and pending queue rows kept for replay until a connector route's retention limits prune them.
_Avoid_: Backlog

**Delivery class**:
The sink capability that defines whether the route boundary is confirmed, resumable, or best-effort.
_Avoid_: Reliability level

**Bridge mode**:
The connector route policy that chooses observe-only, semantic re-origination, or transparent wire-preserving forwarding.
_Avoid_: Sink mode

**Commissioning baseline**:
An operator-approved inventory of expected Device NAMEs and product/configuration details for a vessel installation.
_Avoid_: Discovered devices

## Relationships

- A **Bus endpoint** observes zero or more **Device NAMEs**
- One **Appliance identity** participates on zero or more independent **Bus endpoints**
- A **Connector route** consumes **Envelopes** and has exactly one **Bridge mode** and one **Delivery class**
- **Pending delivery** is a subset of **Retained history**
- A **Commissioning baseline** contains zero or more expected **Device NAMEs** per **Bus endpoint**

## Example dialogue

> **Dev:** "The queue shows 20,000 rows. Is the connector route behind?"
> **Domain expert:** "Check pending delivery, not retained history. Those rows may already be acknowledged and kept only for replay."

## Flagged ambiguities

- "device id" previously meant both source address and ISO NAME — resolved: **Device NAME** is stable identity; source address is the current bus address.
- "queue depth" previously counted all retained rows — resolved: route health reports **Pending delivery**, while replay storage reports **Retained history**.
- "CAN forwarding" previously meant semantic re-origination — resolved: **Bridge mode** states whether source identity and unknown PGNs are preserved.
