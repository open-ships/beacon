# Getting started

This example reads NMEA 2000 messages from one CAN bus and publishes them at
an SSE endpoint.

[![SocketCAN can0 through a connector with CEL filters and a durable buffer to SSE /events](/assets/manual/routing-topology.svg)](/assets/manual/routing-topology.svg)

_The connector is the policy and durability boundary between one source and
one sink._

## Before you start

Make sure that these items are ready:

- Beacon is installed.
- The SocketCAN interface is named `can0`.
- The interface is up at 250 kbit/s.

Use the CAN setup page if you must prepare the interface.

## Start Beacon

Run Beacon with the default admin and data ports:

```bash
beacon --db beacon.db
```

Open `http://localhost:2112` in a browser.

Administration binds to `127.0.0.1:2112` by default. The UI, API, and MCP can
read credentials and change the appliance; they have no built-in login.
For remote access, run this command on your workstation:

```bash
ssh -N -L 12112:127.0.0.1:2112 operator@beacon-host
```

Open `http://127.0.0.1:12112`. SSH supplies authentication and encryption.
Existing installations that need direct LAN access must explicitly set
`--admin-address` and protect the listener with network access controls.

Forms also work without JavaScript. After selecting a source or sink type,
select **Update type fields** before entering its settings, then **Save**.

## Add the CAN source

1. Open **Sources**.
2. Select **Add source**.
3. Set **Name** to `CAN bus`.
4. Set **Type** to `socketcan`.
5. Set **Interface** to `can0`.
6. Enable the source.
7. Save the source.

## Add the SSE sink

1. Open **Sinks**.
2. Select **Add sink**.
3. Set **Name** to `SSE feed`.
4. Set **Type** to `http_sse`.
5. Set **Path** to `/events`.
6. Enable the sink.
7. Save the sink.

## Connect the source to the sink

1. Open **Connectors**.
2. Select **Add connector**.
3. Set **Name** to `CAN to SSE`.
4. Select the CAN source.
5. Select the SSE sink.
6. Keep the filter list empty.
7. Enable the connector.
8. Save the connector.

[![Beacon dashboard showing one running source fanning out through two connector routes to two sinks](/assets/manual/dashboard.png)](/assets/manual/dashboard.png)

_The dashboard traces every configured route from left to right. Dotted lines
connect shared endpoints without hiding which connector owns each route._

## Read the SSE stream

Connect to the data port:

```bash
curl -N http://localhost:8080/events
```

Beacon writes one SSE event for each NMEA 2000 message that arrives on
`can0`. The dashboard shows the live message rate and route state.

Use the Sources, Sinks, and Connectors pages for component settings. Use the
Examples page to create the same route with the API or a virtual CAN bus.
